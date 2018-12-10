import sqlite3
import csv


class SQLConnect:
    def __init__(self, sqlite_path):
        try:
            self.conn = sqlite3.connect(sqlite_path)
        except Exception as e:
            print(e)

    @staticmethod
    def get_ann(split_string, target_string):
        split = split_string.split()
        target = target_string[1:-1].replace("'","").replace(" ","").split(',')
        split.append('#')
        target.append('O')
        print(target)
        assert len(split) == len(target)
        ann_list = []
        idx = 0
        turple = []
        for i in range(0, len(split)-1):
            if target[i][0] == 'B':
                turple.append(target[i][2:])
                turple.append(idx)
                if target[i+1][0] != 'I':
                    turple.append(idx+len(split[i]))
                    ann_list.append(turple)
                    turple = []
            elif target[i][0] == 'I' and target[i+1][0] != 'I':
                if len(turple)==0:
                    continue
                turple.append(idx+len(split[i]))
                ann_list.append(turple)
                turple = []
            idx += len(split[i])+1
        return ann_list

    def import_doc_to_project(self, project_id, doc_begin_id, username, csv_file_path):
        c = self.conn.cursor()
        csv_file = csv.reader(open(csv_file_path, 'r', encoding='utf-8'))
        cursor = c.execute("SELECT id, text FROM server_label WHERE project_id="+str(project_id))
        label_id_dict = {}
        for row in cursor:
            label_id_dict[row[1]] = row[0]
        cursor = c.execute("SELECT id FROM auth_user WHERE username='"+str(username)+"'")
        user_id = None
        for row in cursor:
            user_id = row[0]
        for i, line in enumerate(csv_file):
            assert len(line) == 2
            #中文空格转英文
            line[0] = ' '.join(line[0].split())
            doc_id = doc_begin_id + i
            insert_sql = "INSERT INTO server_document(id, text, project_id) "
            insert_sql += "VALUES(" + ", ".join([str(doc_id), "'"+str(line[0])+"'", str(project_id)]) + ")"
            c.execute(insert_sql)
            ann_list = self.get_ann(str(line[0]), line[1])
            for ann in ann_list:
                label_id = label_id_dict[ann[0]]
                insert_sql = "INSERT INTO server_sequenceannotation(prob, manual, start_offset, end_offset, document_id, label_id, user_id) "
                insert_sql += "VALUES(" + ", ".join(['0','0',str(ann[1]),str(ann[2]),str(doc_id),str(label_id),str(user_id)]) + ")"
                c.execute(insert_sql)

        self.conn.commit()
        print('Import Over!')
        self.conn.close()


obj = SQLConnect('db.sqlite3')
obj.import_doc_to_project(25, 6000, 'eason', '/Users/chenyuanzhe/Desktop/df_news_v05_split_last_top120.csv')
