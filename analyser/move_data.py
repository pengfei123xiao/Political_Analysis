import functional_tools
import sys
import pandas as pd

def move_data(db_name,collection_name):
    try:
        f_tools = functional_tools.FunctionalTools()
        # 203.101.225.125/")  # carol
        # client = MongoClient("mongodb://admin:123@115.146.85.107/")  # backend
        # client = MongoClient("mongodb://admin:123@103.6.254.48/")  # DB
        # read politician data from mongoDB
        # ,"115.146.85.107/"
        loc_from_mongo = f_tools.find_data(db_name, "locToCooOneDay", "203.101.225.125/")
        loc_df = pd.DataFrame(list(loc_from_mongo))
        del loc_from_mongo
        print(loc_df.shape)
        loc_target_from_mongo = f_tools.find_data(db_name, collection_name)
        loc_target_df = pd.DataFrame(list(loc_target_from_mongo))
        print(loc_target_df.shape)
        new_loc_df = loc_df[~ loc_df['Location'].isin(loc_target_df['Location'])]
        # loc_df.set_index('Location').subtract(loc_target_df.set_index('Location'), fill_value=0)
        del loc_target_from_mongo
        print(new_loc_df.shape)
        f_tools.save_data(new_loc_df.to_dict('records'), 'backup', collection_name,'insert_many')
    except Exception as e:
        print(e)


def drop_duplicate(db_name,collection_name):
    try:
        f_tools = functional_tools.FunctionalTools()
        # 203.101.225.125/")  # carol
        # client = MongoClient("mongodb://admin:123@115.146.85.107/")  # backend
        # client = MongoClient("mongodb://admin:123@103.6.254.48/")  # DB
        # read politician data from mongoDB
        # ,"115.146.85.107/"
        loc_from_mongo = f_tools.find_data(db_name, collection_name)
        loc_df = pd.DataFrame(list(loc_from_mongo))
        del loc_from_mongo
        print(loc_df.shape)
        loc_df.drop_duplicates(subset ="Location",
                     keep = "first", inplace = True)
        print(loc_df.shape)
        # f_tools.save_data(loc_df.to_dict('records'), 'backup', "locToCooBAKK",'insert_many')
    except Exception as e:
        print(e)

if __name__ == '__main__':
    db_name = sys.argv[1]
    collection_name = sys.argv[2]
    print(db_name)
    print(collection_name)
    move_data(db_name,collection_name)
    # drop_duplicate(db_name,collection_name)
