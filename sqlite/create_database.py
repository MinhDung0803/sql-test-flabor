import sqlite3
import os
import yaml


# get configuration file's information
config = yaml.load(open('config.yaml'), Loader=yaml.SafeLoader)


def final_create_db():
    global config
    dbname_ = config["database_name"]+".db"
    if os.path.exists(dbname_):
        os.remove(dbname_)
    conn = sqlite3.connect(dbname_)
    c = conn.cursor()
    # Create table
    c.execute('''CREATE TABLE UUIDS([uuid] TEXT PRIMARY KEY,
                    [name] TEXT DEFAULT NAN,
                    [age] INTEGER DEFAULT NAN,
                    [gender] TEXT DEFAULT NAN,
                    [level] INTEGER DEFAULT 1,
                    [master_user] INTEGER NOT NULL,
                    [regis_masked] INTEGER NOT NULL,
                    [regis_non_masked] INTEGER NOT NULL,
                    [updated_at] TEXT NOT NULL,
                    [created_at] TEXT NOT NULL)''')

    c.execute('''CREATE TABLE EMBEDDINGS([uuid] TEXT NOT NULL,
                    [feature_vector] TEXT NOT NULL,
                    [masked_face] INTEGER NOT NULL,
                    [image] BLOB NOT NULL,
                    [bbox_tlx] INTEGER NOT NULL,
                    [bbox_tly] INTEGER NOT NULL,
                    [bbox_brx] INTEGER NOT NULL,
                    [bbox_bry] INTEGER NOT NULL,
                    [updated_at] TEXT NOT NULL,
                    [created_at] TEXT NOT NULL,
                    FOREIGN KEY(uuid) REFERENCES UUIDS(uuid))''')

    c.execute(f'''CREATE TABLE CHECKIN([uuid] TEXT NOT NULL,
                    [checkin_time] TEXT NOT NULL,
                    [checkin_image] BLOB NOT NULL,
                    [checkin_vector] TEXT NOT NULL,
                    [checkin_bbox_tlx] INTEGER NOT NULL,
                    [checkin_bbox_tly] INTEGER NOT NULL,
                    [checkin_bbox_brx] INTEGER NOT NULL,
                    [checkin_bbox_bry] INTEGER NOT NULL,
                    [checkin_masked] INTEGER NOT NULL,
                    FOREIGN KEY(uuid) REFERENCES UUIDS(uuid))''')
    conn.commit()


# final_create_db()