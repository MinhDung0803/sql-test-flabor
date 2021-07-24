import sqlite3
import time
from datetime import datetime
import pandas as pd
import numpy as np
import yaml
import os
import create_database

# get configuration file's information
config = yaml.load(open('config.yaml'), Loader=yaml.SafeLoader)


def convert_to_binary_data(filename):
    """
    Convert image to save in DB(for image)
    :param filename:
    :return:
    """
    # time_start = datetime.now()
    with open(filename, 'rb') as file:
        blob_data = file.read()
    # time_stop = datetime.now()
    # count_time = (time_stop - time_start)
    # total_seconds = count_time.total_seconds()
    # print("[INFO]-- Cost time to convert image", total_seconds)
    return blob_data


def list_to_string(input_list):
    """
    Convert list to string(for bbox)
    :param input_list:
    :return:
    """
    str_result = ' '.join([str(elem) for elem in input_list])
    return str_result


def compare_features(feature, all_features):
    """
    Compare feature vector to get the most similarity vector
    :param feature:
    :param all_features:
    :return:
    """
    sims = np.dot(all_features, feature)
    pare_index = np.argmax(sims)
    score = sims[pare_index]
    return score, pare_index


# connect to database
conn = sqlite3.connect(config["database_name"]+".db")
c = conn.cursor()


def insert_db(data, table_name):
    """
    Insert data into DB
    :param data:
    :param table_name:
    :return:
    """
    global conn
    global c

    try:
        time_start = datetime.now()
        data_form_add = pd.DataFrame.from_dict(data)
        data_form_add.to_sql(table_name, conn, if_exists='append', index=False)
        conn.commit()
        time_stop = datetime.now()
        count_time = (time_stop - time_start)
        total_seconds = count_time.total_seconds()
        print("Cost time to insert Data to", table_name, "table: ", total_seconds, "s")
    except sqlite3.Error as error:
        print("Failed to insert Data into", table_name, "table because of error: ", error)


def delete_by_uuid(uuid_, table_name_):
    """
    Using to delete one user(usually spot user) based on its uuid
    :param table_name_:
    :param uuid_:
    :return:
    """
    global conn
    global c

    try:
        delete_query = f"DELETE FROM '{table_name_}' WHERE uuid = '{uuid_}'"
        c.execute(delete_query)
        conn.commit()
        print("Deleted uuid ", uuid_,"at table", table_name_, " - DONE")
    except sqlite3.Error as error:
        print("Failed to Deleted uuid ", uuid_,"at table", table_name_, "because of error: ", error)


def delete_multiple_uuids(list_uuids, table_name):
    """
    Using to delete multiple users based on a list of uuids
    :param table_name:
    :param list_uuids:
    :return:
    """
    global conn
    global c

    try:
        list_of_ques = ", ".join("?" * len(list_uuids))
        delete_multiple_query = f"DELETE FROM '{table_name}' WHERE uuid IN ({list_of_ques})"
        c.execute(delete_multiple_query, list_uuids)
        conn.commit()
        print("Deleted multiple uuids ", list_uuids, " - DONE")
    except sqlite3.Error as error:
        print("Failed to Deleted uuid ", list_uuids, "at table", table_name, "because of error: ", error)


def query_all_vectors_for_matching(masked=1):
    """
    Query all feature vectors of all users(uuids) from database based on masked value
    :param masked:
    :return:
    """
    global conn
    global c

    try:
        query_all_feature_vectors = f"SELECT * FROM EMBEDDINGS WHERE masked_face = '{masked}'"
        time_start = datetime.now()
        c.execute(query_all_feature_vectors)
        return_all_feature_vector = c.fetchall()
        time_stop = datetime.now()
        count_time = (time_stop-time_start)
        total_seconds = count_time.total_seconds()
        print("Cost time for all feature vectors from DB: ", total_seconds, "s")
        conn.commit()
        return return_all_feature_vector
    except sqlite3.Error as error:
        print("Failed to query all vectors for matching because of error: ", error)
        return None


def query_vectors_by_uuid(uuid, masked_st):
    """
    Query all feature vectors by uuids and masked_st(to check for updating vector)
    :param masked_st:
    :param uuid:
    :return:
    """
    global conn
    global c

    try:
        query_vectors_uuid = f"SELECT * FROM EMBEDDINGS WHERE uuid = '{uuid}' and masked_face = '{masked_st}'"
        time_start = datetime.now()
        c.execute(query_vectors_uuid)
        return_all_vectors = c.fetchall()
        time_stop = datetime.now()
        count_time = (time_stop - time_start)
        total_seconds = count_time.total_seconds()
        print("Cost time for querying feature vectors of ", uuid, "from DB: ", total_seconds, "s")
        conn.commit()
        return return_all_vectors
    except sqlite3.Error as error:
        print("Failed to query feature vectors of ", uuid, "from DB because of error: ", error)
        return None


def update_oldest_vector(uuid_, new_vector):
    """
    Update oldest vector by new vector at specific uuid
    :param uuid_:
    :param new_vector:
    :return:
    """
    global conn
    global c

    try:
        update_query = f"UPDATE EMBEDDINGS SET feature_vector = '{new_vector}', updated_at = '{str(datetime.now())}' " \
                       f"WHERE uuid = '{uuid_}' AND updated_at = (SELECT updated_at FROM EMBEDDINGS " \
                       f"WHERE uuid = '{uuid_}' ORDER BY updated_at ASC LIMIT 1)"
        c.execute(update_query)
        conn.commit()
        print("Update oldest vector for uuid: ", uuid_, " -DONE")
    except sqlite3.Error as error:
        print("Failed to update vector for: ", uuid_, "because of error: ", error)
        return None


def update_regis_status(uuid_, regis_infor):
    global conn
    global c

    try:
        update_value = 1
        update_regis_query = f"UPDATE UUIDS SET '{regis_infor}' = '{update_value}', " \
                             f"updated_at = '{str(datetime.now())}' WHERE uuid = '{uuid_}' "
        c.execute(update_regis_query)
        conn.commit()
        print("Update oldest vector for uuid: ", uuid_, " -DONE")
    except sqlite3.Error as error:
        print("Failed to update register masked face status for: ", uuid_, "because of error: ", error)


def query_five_latest_vectors_for_matching():
    """
        Query all feature vectors of all users(uuids) from database based on masked value
        :return: query results
        """
    global conn
    global c

    try:
        uuid_list = ["uuid0", "uuid1", "uuid2"]
        query_all_feature_vectors = "SELECT uuid, updated_at FROM EMBEDDINGS WHERE uuid in (SELECT uuid FROM UUIDS) " \
                                    "ORDER BY updated_at ASC LIMIT 5"
        # query_all_feature_vectors = "SELECT uuid FROM UUIDS"

        # query_ex = "SELECT * FROM( SELECT * FROM BOOK, AUTHOR WHERE BOOK.AUTHORID = AUTHOR.AUTHORID) T1 " \
        #            "WHERE T1.BOOKID IN( SELECT T2.BOOKID FROM BOOK T2 WHERE T2.AUTHORID = T1.AUTHORID ORDER " \
        #            "BY T2.BOOKTITLE LIMIT 2 ) ORDER BY T1.BOOKTITLE"
        time_start = datetime.now()
        c.execute(query_all_feature_vectors)
        return_all_feature_vector = c.fetchall()
        time_stop = datetime.now()
        count_time = (time_stop - time_start)
        total_seconds = count_time.total_seconds()
        print("Cost time for all feature vectors from DB: ", total_seconds, "s")
        conn.commit()
        return return_all_feature_vector
    except sqlite3.Error as error:
        print("Failed to query all vectors for matching because of error: ", error)
        return None


def query_from_db_test(name_of_table):
    """
    Query all feature vector from database based on masked value FOR TESTING
    :param name_of_table:
    :return:
    """
    global conn
    global c

    select_all_query = f"SELECT * FROM '{name_of_table}'"
    time_start = datetime.now()
    c.execute(select_all_query)
    result_select_all = c.fetchall()
    time_stop = datetime.now()
    count_time = (time_stop-time_start)
    total_seconds = count_time.total_seconds()
    print("[INFO]-- Cost time for query data from local database", total_seconds)
    conn.commit()
    return result_select_all


def get_uuid_data_form(uuid_, name_, age_, gender_, level_, master_user_):
    """
    Convert uuid_data to form to insert into DB
    :param uuid_:
    :param name_:
    :param age_:
    :param gender_:
    :param level_:
    :param master_user_:
    :return:
    """
    uuid_data_form = {
        "uuid": [uuid_],
        "name": [name_],
        "age": [age_],
        "gender": [gender_],
        "level": [level_],
        "master_user": [master_user_],
        "regis_masked": 0,
        "regis_non_masked": 0,
        "updated_at": [str(datetime.now())],
        "created_at": [str(datetime.now())]
    }
    return uuid_data_form


def get_embeddings_data_form(uuid_, feature_vector_, masked_face_, iamge_, bbox_):
    """
    Convert embeddings_data to form to insert into DB
    :param uuid_:
    :param feature_vector_:
    :param masked_face_:
    :param iamge_:
    :param bbox_:
    :return:
    """
    converted_feature_vector = feature_vector_.tostring()
    converted_image = convert_to_binary_data(iamge_)

    embeddings_data_form = {
        "uuid": [uuid_],
        "feature_vector": [converted_feature_vector],
        "masked_face": [masked_face_],
        "image": [converted_image],
        "bbox_tlx": [bbox_[0]],
        "bbox_tly": [bbox_[1]],
        "bbox_brx": [bbox_[2]],
        "bbox_bry": [bbox_[3]],
        "updated_at": [str(datetime.now())],
        "created_at": [str(datetime.now())]
    }

    return embeddings_data_form


def get_checkin_data_form(uuid_, checkin_image_, checkin_vector_, checkin_bbox_, checkin_masked_):
    """
    Convert checkin_data to form to insert into DB
    :param uuid_:
    :param checkin_image_:
    :param checkin_vector_:
    :param checkin_bbox_:
    :param checkin_masked_:
    :return: formed checkin_data
    """
    converted_checkin_image = convert_to_binary_data(checkin_image_)
    converted_feature_vector = checkin_vector_.tostring()

    checkin_data_form = {
        "uuid": [uuid_],
        "checkin_time": [str(datetime.now())],
        "checkin_image": [converted_checkin_image],
        "checkin_vector": [converted_feature_vector],
        "checkin_bbox_tlx": [checkin_bbox_[0]],
        "checkin_bbox_tly": [checkin_bbox_[1]],
        "checkin_bbox_brx": [checkin_bbox_[2]],
        "checkin_bbox_bry": [checkin_bbox_[3]],
        "checkin_masked": [checkin_masked_],
    }

    return checkin_data_form


if __name__ == "__main__":
    if not os.path.exists(config["database_name"]+".db"):
        create_database.final_create_db()

    number_uuid = 3
    uuid_data_ = {
        "uuid": ["uuid"+str(i) for i in range(number_uuid)],
        "name": ["name_0" for i in range(number_uuid)],
        "age": [i+20 for i in range(number_uuid)],
        "gender": ['male' for _ in range(number_uuid)],
        "level": [1 for _ in range(number_uuid)],
        "master_user": [1 for _ in range(number_uuid)],
        "regis_masked": [0 for _ in range(number_uuid)],
        "regis_non_masked": [0 for _ in range(number_uuid)],
        "updated_at": [str(datetime.now()) for _ in range(number_uuid)],
        "created_at": [str(datetime.now()) for _ in range(number_uuid)]
    }

    # insert_db(uuid_data_, "UUIDS")
    result_ = query_from_db_test("UUIDS")
    # print("result: ", len(result_))
    result_ = [result_[i][0] for i in range(len(result_))]
    print("uuid table result: ", result_)

    image_path = "./avatar.jpg"
    bbox_ = [1, 2, 3, 4]
    num_embed = 7
    list_of_uuid = [str("uuid"+str(i)) for i in range(3)]

    # for uuid__ in list_of_uuid:
    #     embeddings_data_ = {
    #         "uuid": [uuid__ for _ in range(num_embed)],
    #         "feature_vector": [np.random.rand(512).tostring() for _ in range(num_embed)],
    #         "masked_face": [1 for _ in range(num_embed)],
    #         "image": [convert_to_binary_data(image_path) for _ in range(num_embed)],
    #         "bbox_tlx": [bbox_[0] for _ in range(num_embed)],
    #         "bbox_tly": [bbox_[1] for _ in range(num_embed)],
    #         "bbox_brx": [bbox_[2] for _ in range(num_embed)],
    #         "bbox_bry": [bbox_[3] for _ in range(num_embed)],
    #         "updated_at": [str(datetime.now()) for _ in range(num_embed)],
    #         "created_at": [str(datetime.now()) for _ in range(num_embed)]
    #     }
    #
    #     insert_db(embeddings_data_, "EMBEDDINGS")

    result_e = query_from_db_test("EMBEDDINGS")
    print(len(result_e[0]))
    result_uuid = [result_e[i][0] for i in range(len(result_e))]
    print("embeddings table result: ", result_uuid, len(result_uuid))

    result_time = [result_e[i][8] for i in range(len(result_e))]
    print("embeddings table result - time: ", result_time, len(result_time))

    query_5_result = query_five_latest_vectors_for_matching()
    print(query_5_result)

    # checkin_image_path = "./avatar.jpg"
    # checkin_number = 10
    # checkin_bbox_ = [5, 6, 7, 8]
    # checkin_data_ = {
    #     "uuid": ["uuid_2" for i in range(checkin_number)],
    #     "checkin_time": [str(datetime.now()) for _ in range(checkin_number)],
    #     "checkin_image": [convert_to_binary_data(checkin_image_path) for _ in range(checkin_number)],
    #     "checkin_vector": [np.random.rand(512).tostring() for _ in range(checkin_number)],
    #     "checkin_bbox_tlx": [checkin_bbox_[0] for _ in range(checkin_number)],
    #     "checkin_bbox_tly": [checkin_bbox_[1] for _ in range(checkin_number)],
    #     "checkin_bbox_brx": [checkin_bbox_[2] for _ in range(checkin_number)],
    #     "checkin_bbox_bry": [checkin_bbox_[3] for _ in range(checkin_number)],
    #     "checkin_masked": [1 for _ in range(checkin_number)],
    # }

    # result_vector = query_from_db_test()
    # # print("All vectors: ", result_vector, len(result_vector))
    # final_result = [result_vector[i][0] for i in range(len(result_vector))]
    # print("before result: ", final_result)
    #
    # # uuid_in = "uuid_0"
    # # delete_uuid(uuid_in)
    #
    # list_of_uuids = ["uuid_0"]
    # delete_multiple_uuid(list_of_uuids)
    #
    # result_vector = query_from_db_test()
    # # print("All vectors: ", result_vector, len(result_vector))
    # final_result = [result_vector[i][0] for i in range(len(result_vector))]
    # print("after result: ", final_result)
