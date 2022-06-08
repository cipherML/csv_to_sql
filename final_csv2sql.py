import re
import warnings
import pandas as pd
from datetime import datetime
from datetime import date

# from mediadb_database import Database

warnings.filterwarnings("ignore")

now = datetime.now()
today = date.today()
today = today.strftime("%d_%m_%Y")
dt_string = now.strftime("%Y-%m-%d %H:%M:%S")

# input csv (input file path) # RECTIFY THE SQL NAME WITH SPACES AND DASHES #handle linebreak
file_path = r"sample_input.csv" 

# rep_nn_.csv
# out_nu_.csv

data = pd.read_csv(file_path, skipinitialspace=True, encoding="utf-8")

sql_name = file_path.split(".")[0]+'_'+str(today) # TO:DO date first, duplication, uniq column, name diff..zz 
print(sql_name)
f = open(sql_name+".sql", "a", encoding="utf-8")

# Data processing
data = data.fillna(0)
data = data.dropna(how='all')
data = data.replace(r'\r', regex=True)
data = data.replace(r'\n', regex=True)
data = data.replace("'", "''")

# Create a error dict
# duplicate_records = pd.DataFrame(columns=data.columns)
error_dict = pd.DataFrame(columns=data.columns)

# def check_duplicates():
#     global duplicate_records
#     reporter_db = []
#     connect = Database()
#     reporter_a = connect.select_rows_dict_cursor(
#         "SELECT reporter_id, email, twitter, facebook, linkedin, instagram FROM tbl_reporters;")
#     for i in reporter_a:
#         reporter_db.append(i)
#     return reporter_db
#
#
# r = check_duplicates()
#
# df = pd.DataFrame(r, columns=['reporter_id', 'email', 'twitter', 'facebook', 'linkedin', 'instagram'])
#
# # check duplicates in db
# # Email duplicates
# try:
#     for e_index, email in enumerate(data["email"]):
#         if email in df['email'].values:
#             duplicate_records = duplicate_records.append(data.iloc[e_index])
#             # duplicate_records["found_in"]= "email"
#             data.drop(data.index[e_index])
#
#     # twitter duplicates
#     for t_index, twitter in enumerate(data["twitter"]):
#         if twitter in df["twitter"].values:
#             duplicate_records = duplicate_records.append(data.iloc[t_index])
#             # duplicate_records["found_in"] = "twitter"
#             data.drop(data.index[t_index])
#
#     # Facebook duplicates
#     for f_index, facebook in enumerate(data["facebook"]):
#         if facebook in df["facebook"].values:
#             duplicate_records = duplicate_records.append(data.iloc[f_index])
#             # duplicate_records["found_in"] = "twitter"
#             data.drop(data.index[f_index])
#
#     # Instagram duplicates
#     for i_index, instagram in enumerate(data["instagram"]):
#         if instagram in df["instagram"].values:
#             duplicate_records = duplicate_records.append(data.iloc[i_index])
#             # duplicate_records["found_in"] = "instagram"
#             data.drop(data.index[i_index])
#
#     # Linkedin duplicates
#     for l_index, linkedin in enumerate(data["linkedin"]):
#         if linkedin in df["linkedin"].values:
#             duplicate_records = duplicate_records.append(data.iloc[l_index])
#             # duplicate_records["found_in"] = "linkedin"
#             data.drop(data.index[l_index])
# except KeyError:
#     pass
#

#
# # reporter_id & outlet_id only numbers, No special character
# pd.to_numeric(data['reporter_id'], errors='coerce').notnull().all()
# # pd.to_numeric(data['outlet_id'], errors='coerce').notnull().all()
# data["reporter_id"] = data["reporter_id"].astype(int)
# # data["outlet_id"] = data["outlet_id"].astype(int)
#
#
# social media validations
email_regex = r"\b[A-Za-z\d._%+-]+@[A-Za-z\d.-]+\.[A-Z|a-z]{2,}\b"
twitter_regex = r"https?://(?:www\.)?twitter\.com/(\w+)"
facebook_regex = r"(?:(?:http|https)://)?(?:www.)?facebook.com/(?:\w*#!/)?(?:pages/)?(?:[?\w\-]*/)?(?:profile.php\?id=(?=\d.*))?([\w\-]*)?"
instagram_regex = r"(?:(?:http|https)://)?(?:www.)?(?:instagram.com|instagr.am|instagr.com)/(\w+)"


# Email validation
def email_check(sm_url, ind):
    if re.search(email_regex, sm_url):
        pass
    else:
        return ind


try:
    for i, email_ in enumerate(data["email"]):
        email_index = email_check(email_, ind=i)
        error_dict = error_dict.append(data.iloc[email_index])
        error_dict["found_in"][email_index] = "email"
        data.drop(data.index[email_index])
except (TypeError, KeyError):
    pass


# Twitter validation
def twitter_check(twitter_id, ind):
    if re.search(twitter_regex, twitter_id):
        pass
    else:
        return ind


try:
    for j, twitter in enumerate(data["twitter"]):
        twitter_index = twitter_check(twitter_id=twitter, ind=j)
        error_dict = error_dict.append(data.iloc[twitter_index])
        error_dict["found_in"][twitter_index] = "twitter"
        data.drop(data.index[twitter_index])
except (TypeError, KeyError):
    pass


# Facebook validation
def facebook_check(facebook_id, ind):
    if re.search(facebook_regex, facebook_id):
        pass
    else:
        return ind


try:
    for k, fb in enumerate(data["facebook"]):
        fb_index = facebook_check(facebook_id=fb, ind=k)
        error_dict = error_dict.append(data.iloc[fb_index])
        error_dict["found_in"][fb_index] = "facebook"
        data.drop(data.index[fb_index])
except (TypeError, KeyError):
    pass


# Instagram validation
def instagram_check(instagram_id, ind):
    if re.search(instagram_regex, instagram_id):
        pass
    else:
        return ind


try:
    for l, insta in enumerate(data["instagram"]):
        insta_index = instagram_check(instagram_id=insta, ind=l)
        error_dict = error_dict.append(data.iloc[insta_index])
        error_dict["found_in"][insta_index] = "instagram"
        data.drop(data.index[insta_index])
except (TypeError, KeyError):
    pass

error_dict.to_csv("error.csv")
# duplicate_records.to_csv("duplicate_rocords.csv")


def db_query_field(i):
    try:
        outlet_id = f"outlet_id =  {str(data['outlet_id'][i])} , "
    except KeyError:
        outlet_id = None

    try:
        id = f"id =  {str(data['id'][i])} , "
    except KeyError:
        id = None

    try:
        email_status_code = f"email_status_code =  '{str(data['email_status_code'][i])}' , "
    except KeyError:
        email_status_code = None

    try:
        followers = f"followers =  {str(data['followers'][i])} , "
    except KeyError:
        followers = None

    try:
        first_name = "first_name = " + "'" + str(data["first_name"][i]) + "'" + ", "
    except KeyError:
        first_name = None

    try:
        last_name = "last_name = " + "'" + str(data["last_name"][i]) + "'" + ", "
    except KeyError:
        last_name = None

    try:
        name = "name = " + "'" + str(data["name"][i]) + "'" + ", "
        # name = f"name = '{str(data['name'][i])}' , "
    except KeyError:
        name = None

    try:
        handle = f'{str(data["handle"][i]).lower()},'
    except KeyError:
        handle = None
    try:
        email = f"email = '{str(data['email'][i])}' , "
    except KeyError:
        email = None

    try:
        emailed_date = f"emailed_date = '{str(data['emailed_date'][i])}' , "
    except KeyError:
        emailed_date = None

    try:
        reporter_email_pattern = f"reporter_email_pattern = '{str(data['reporter_email_pattern'][i])}' , "
    except KeyError:
        reporter_email_pattern = None

    try:
        website = f"'{str(data['website'][i])}'"
    except KeyError:
        website = None

    try:
        personal_website = f"personal_website = '{str(data['personal_website'][i])}' , "
    except KeyError:
        personal_website = None
    try:
        url = f"url = '{str(data['url'][i])}' , "
    except KeyError:
        url = None

    try:
        twitter_url = f"'{str(data['twitter_url'][i])}',"
    except KeyError:
        twitter_url = None

    try:
        reporter_url = f"reporter_url = '{str(data['reporter_url'][i])}' , "
    except KeyError:
        reporter_url = None

    try:
        designation = f"designation = '{str(data['designation'][i])}' , "
    except KeyError:
        designation = None
    try:
        description = f"description = '{str(data['description'][i])}' , "
    except KeyError:
        description = None

    try:
        twitter_description = f"twitter_description = '{str(data['twitter_description'][i])}' , "
    except KeyError:
        twitter_description = None

    try:
        twitter = f"twitter = '{str(data['twitter'][i])}' , "
    except KeyError:
        twitter = None
    try:
        facebook = "facebook = " + "'" + str(data["facebook"][i]) + "'" + ", "
    except KeyError:
        facebook = None
    try:
        linkedin = f"linkedin = '{str(data['linkedin'][i])}' , "
    except KeyError:
        linkedin = None
    try:
        instagram = f"instagram = '{str(data['instagram'][i])}' , "
    except KeyError:
        instagram = None
    try:
        youtube = f"youtube = '{str(data['youtube'][i])}' , "
    except KeyError:
        youtube = None
    try:
        flickr = f"flickr = '{str(data['flickr'][i])}' , "
    except KeyError:
        flickr = None
    try:
        pinterest = f"pinterest = '{str(data['pinterest'][i])}' , "
    except KeyError:
        pinterest = None
    try:
        wikipedia = f"wikipedia = '{str(data['wikipedia'][i])}' , "
    except KeyError:
        wikipedia = None
    try:
        tiktok = f"tiktok = '{str(data['tiktok'][i])}' , "
    except KeyError:
        tiktok = None

    try:
        snapchat = f"snapchat = '{str(data['snapchat'][i])}' , "
    except KeyError:
        snapchat = None

    try:
        location = "'" + str(data["location"][i]) + "'" + ","
    except KeyError:
        location = None

    try:
        location_url = "location_url = " + "'" + str(data["location_url"][i]) + "'" + ", "
    except KeyError:
        location_url = None

    try:
        assn_rawData = str(data["associations"][i]).replace(" ", "").replace("  ", "").split(
            ",")  # removing spaces and converting one string into an array of strings seperated by comma (,)
        assn_cleanData = str(set(assn_rawData)).replace("'",
                                                        "")
        # converting rawData into set to get rid of redundant data ,
        # and removing quotes inserted by set transformation to make cleanData SQL Valid.
        associations = f"associations = ''{str(assn_cleanData)}'' , "

    except KeyError:
        associations = None

    try:
        topic_id = f"topic_id = '{str(data['topic_id'][i])}' , "

    except KeyError:
        topic_id = None

    try:
        country_code = f"country_code = '{str(data['country_code'][i])}' , "
    except KeyError:
        country_code = None

    try:
        state = f"state = '{str(data['state'][i])}' , "
    except KeyError:
        state = None
    try:
        city = f"city = '{str(data['city'][i])}' , "
    except KeyError:
        city = None

    try:
        address = f"address = '{str(data['address'][i])}' , "
    except KeyError:
        address = None

    try:
        active = f"active = {str(data['active'][i])} , "
    except KeyError:
        active = None

    try:
        cm_syndicated = f"cm_syndicated = {str(data['cm_syndicated'][i])} , "
    except KeyError:
        cm_syndicated = None

    try:
        mobile_number = f"mobile_number = '{str(data['mobile_number'][i])}' , "
    except KeyError:
        mobile_number = None

    try:
        phone_number = f"phone_number = '{str(data['phone_number'][i])}' , "
    except KeyError:
        phone_number = None

    try:
        contact_us_url = f"contact_us_url = '{str(data['contact_us_url'][i])}' , "
    except KeyError:
        contact_us_url = None

    try:
        staff_url = f"staff_url = '{str(data['staff_url'][i])}' , "
    except KeyError:
        staff_url = None

    try:
        zip = f"zip = '{str(data['zip'][i])}' , "
    except KeyError:
        zip = None
    try:
        zipcode = f"zipcode = '{str(data['zipcode'][i])}' , "
    except KeyError:
        zipcode = None
    try:
        topics = f"topics = '{str(data['topics'][i])}' , "
    except KeyError:
        topics = None
    try:
        muckrack = f"muckrack = '{str(data['muckrack'][i])}' , "
    except KeyError:
        muckrack = None
    try:
        pitch = f"pitch = '{str(data['pitch'][i])}' , "
    except KeyError:
        pitch = None
    try:
        profile_pic = f"profile_pic = '{str(data['profile_pic'][i])}' , "
    except KeyError:
        profile_pic = None
    try:
        logo = f"logo = '{str(data['logo'][i])}' , "
    except KeyError:
        logo = None

    try:
        keywords = f"keywords = '{str(data['keywords'][i])}' , "
    except KeyError:
        keywords = None

    try:
        cm_channel_id = f"cm_channel_id = '{str(data['cm_channel_id'][i])}' , "
    except KeyError:
        cm_channel_id = None

    try:
        contact_type = f"contact_type = '{str(data['contact_type'][i])}' , "
    except KeyError:
        contact_type = None

    try:
        preferred_contact_method = f"preferred_contact_method = '{str(data['preferred_contact_method'][i])}' , "
    except KeyError:
        preferred_contact_method = None

    try:
        also_known_as = f"also_known_as = '{str(data['also_known_as'][i])}' , "
    except KeyError:
        also_known_as = None

    try:
        mediatype = f"mediatype = '{str(data['mediatype'][i])}' , "
    except KeyError:
        mediatype = None

    try:
        cm_outlet_id = f"cm_outlet_id = '{str(data['cm_outlet_id'][i])}' , "
    except KeyError:
        cm_outlet_id = None

    try:
        dma_name = f"dma_name = '{str(data['dma_name'][i])}' , "
    except KeyError:
        dma_name = None

    try:
        dma_code = f"dma_code = '{str(data['dma_code'][i])}' , "
    except KeyError:
        dma_code = None

    try:
        dma_id = f"dma_id = '{str(data['dma_id'][i])}' , "
    except KeyError:
        dma_id = None

    try:
        dma = f"dma = '{str(data['dma'][i])}' , "
    except KeyError:
        dma = None

    try:
        fips = f"fips = '{str(data['fips'][i])}' , "
    except KeyError:
        fips = None

    try:
        focus = f"focus = '{str(data['focus'][i])}' , "
    except KeyError:
        focus = None

    try:
        county = f"county = '{str(data['county'][i])}' , "
    except KeyError:
        county = None

    try:
        st = f"st = '{str(data['st'][i])}' , "
    except KeyError:
        st = None

    """ ___________________________ PLEASE MENTION THE COLUMN NAMES HERE _____________________"""
    # Example
    columns = cm_channel_id
    return columns


for index in data.index:
    q = db_query_field(index)

    # query = "Update tbl_reporters set " + q + "last_updated= " + "'" + str(
    #     dt_string) + "'" + " where reporter_id = " + str(data["reporter_id"][index]) + ";"

    query = "Update tbl_outlets set " + q + "last_updated= " + "'" + str(
        dt_string) + "'" + " where id = " + str(data["outlet_id"][index]) + ";"

    # query = "Update tbl_twitters set " + q + "last_updated= " + "'" + str(
    #     dt_string) + "'" + " where handle = " + str(data["handle"][index]) + ";"
    query = query.replace(".0", "")
    query = query.replace("'{", '')
    query = query.replace("}'", '')
    query = query.replace("'0'", "NULL")
    print(query)

    f.write(query + "\n")
f.close()
