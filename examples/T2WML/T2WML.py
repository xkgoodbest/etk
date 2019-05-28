import yaml, re
from etk.etk import ETK
from etk.extractors.excel_extractor import ExcelExtractor
from etk.knowledge_graph import KGSchema, URI, Literal, LiteralType, Subject, Reification
from etk.etk_module import ETKModule
from etk.wikidata.entity import WDProperty, WDItem
from etk.wikidata.value import Datatype, Item, TimeValue, Precision, QuantityValue
from etk.wikidata import serialize_change_record


class T2WML():

    def __init__(self):
        self.value_dict = {'value': '$col,$row',
                           'from_row': '$row',
                           'from_col': '$col'}
        self.ee = ExcelExtractor()
        self.init_doc()

    def get_exec_res(self, code):
        exec('global res; res = %s' % code)
        global res
        return res

    def create_property(self):
        property = self.schema.get('statementMapping')[1].get('property')
        p = WDProperty(property, Datatype.QuantityValue)
        self.doc.kg.add_subject(p)
        return property

    def get_unit(self):
        u = self.schema.get('statementMapping')[1].get('unit')
        unit = WDItem(u)
        return unit

    def init_doc(self):

        # initialize
        kg_schema = KGSchema()
        kg_schema.add_schema('@prefix : <http://isi.edu/> .', 'ttl')
        etk = ETK(kg_schema=kg_schema, modules=ETKModule)
        self.doc = etk.create_document({}, doc_id="http://isi.edu/default-ns/projects")

        # bind prefixes
        self.doc.kg.bind('wikibase', 'http://wikiba.se/ontology#')
        self.doc.kg.bind('wd', 'http://www.wikidata.org/entity/')
        self.doc.kg.bind('wdt', 'http://www.wikidata.org/prop/direct/')
        self.doc.kg.bind('wdtn', 'http://www.wikidata.org/prop/direct-normalized/')
        self.doc.kg.bind('wdno', 'http://www.wikidata.org/prop/novalue/')
        self.doc.kg.bind('wds', 'http://www.wikidata.org/entity/statement/')
        self.doc.kg.bind('wdv', 'http://www.wikidata.org/value/')
        self.doc.kg.bind('wdref', 'http://www.wikidata.org/reference/')
        self.doc.kg.bind('p', 'http://www.wikidata.org/prop/')
        self.doc.kg.bind('pr', 'http://www.wikidata.org/prop/reference/')
        self.doc.kg.bind('prv', 'http://www.wikidata.org/prop/reference/value/')
        self.doc.kg.bind('prn', 'http://www.wikidata.org/prop/reference/value-normalized/')
        self.doc.kg.bind('ps', 'http://www.wikidata.org/prop/statement/')
        self.doc.kg.bind('psv', 'http://www.wikidata.org/prop/statement/value/')
        self.doc.kg.bind('psn', 'http://www.wikidata.org/prop/statement/value-normalized/')
        self.doc.kg.bind('pq', 'http://www.wikidata.org/prop/qualifier/')
        self.doc.kg.bind('pqv', 'http://www.wikidata.org/prop/qualifier/value/')
        self.doc.kg.bind('pqn', 'http://www.wikidata.org/prop/qualifier/value-normalized/')
        self.doc.kg.bind('skos', 'http://www.w3.org/2004/02/skos/core#')
        self.doc.kg.bind('prov', 'http://www.w3.org/ns/prov#')
        self.doc.kg.bind('schema', 'http://schema.org/')

    def read_yaml(self, yaml_path):
        with open(yaml_path, 'r') as stream:
            data_loaded = yaml.safe_load(stream)
        return data_loaded

    def extract_cells(self, sc, ec, sr, er, file_path):
        start, end = self.region_calc(sc, ec, sr, er)
        extractions = self.ee.extract(file_name=file_path,
                                      sheet_name=self.sheet,
                                      region=[start, end],
                                      variables=self.value_dict)
        return extractions

    def get_cols_rows(self):
        region_dict = self.schema.get('statementMapping')[0]
        sc = region_dict.get('startColumn')
        ec = region_dict.get('endColumn')
        sr = region_dict.get('startRow')
        er = region_dict.get('endRow')
        return sc, ec, sr, er

    def get_Qnodes(self, sr, er):
        # todo
        itemQ = ['Q' + str(i) for i in range(sr, er + 1)]
        return itemQ

    def get_item_Qs(self, sr, er):
        temp = self.schema.get('statementMapping')[1].get('item').split('$')
        item_col = temp[0].split('(')[1]
        itemQs = self.get_Qnodes(sr, er)
        return itemQs

    def region_calc(self, start_col, end_col, start_row, end_row):
        if end_col[-1] == 'Z':
            end_col.append('A')
        else:
            end_col = end_col[:-1] + chr(ord(end_col[-1]) + 1)
        start = ','.join([str(start_col), str(start_row)])
        end = str(end_col) +','+ str(int(end_row) + 1)
        return start, end

    def cell_value(self, col, row):
        start, end = self.region_calc(col, col, row, row)
        extractions = self.ee.extract(file_name=self.file_path,
                                      sheet_name=self.sheet,
                                      region=[start, end],
                                      variables=self.value_dict)

        res = extractions[0]['value']

        return res

    def get_qualifiers(self, col, row):
        res = list()

        quals = self.schema.get('statementMapping')[1].get('qualifier')
        for q in quals:
            qualifier = None
            property = q.get('property')
            value = q.get('value')
            type = q.get('type')
            calendar = Item(q.get('calendar'))
            precision = self.get_exec_res('Precision.' + q.get('precision'))
            time_zone = q.get('time_zone')
            try:
                command = value[2:-2]
                command = command.replace('$col', str(col))
                command = command.replace('$row', str(row))
                value = self.get_exec_res(command)
            except:
                pass
            if type == 'time':
                qualifier = TimeValue(value, calendar=calendar, precision=precision, time_zone=time_zone)
            # other types, todo
            res.append((property, qualifier))
        return res

    def add_Qnodes(self, itemQs, values, property=None, unit=None):
        for i, v in zip(itemQs, values):
            q = WDItem(i)
            s = q.add_statement(property, QuantityValue(v['value'], unit=unit))
            col, row = v['from_col'], v['from_row']
            prop_quals = self.get_qualifiers(col, row)
            for p_q in prop_quals:
                s.add_qualifier(p_q[0], p_q[1])
            self.doc.kg.add_subject(q)


    def runner(self, yaml_path, excel_path, sheet):
        self.sheet = sheet
        self.file_path = excel_path
        self.schema = self.read_yaml(yaml_path)

        p = self.create_property()
        unit = self.get_unit()

        # startColumn, endColumn, startRow, endRow
        sc, ec, sr, er = self.get_cols_rows()

        itemQs = self.get_item_Qs(sr, er)
        values = self.extract_cells(sc, ec, sr, er, excel_path)
        self.add_Qnodes(itemQs, values, p, unit)

        print(self.doc.kg.serialize('ttl'))


if __name__ == '__main__':
    yaml_path = 'sample.yaml'
    excel_path = 'alabama.xls'
    sheet = '16tbl08al'
    converter = T2WML()
    converter.runner(yaml_path, excel_path, sheet)
