import pandas as pd
import pyarrow

pd.options.display.width= None
pd.options.display.max_columns= None
pd.set_option('display.max_rows', 3000)
pd.set_option('display.max_columns', 3000)


def slicing_the_list(data,chat,user,session_id,date) :

    count = 0
    for d in data:
        a=d['msg_type']
        count= count+1
        if a == 'Exception Message':

            content= {
                "user":user,
                "session_id":session_id,
                "date":date,
                "user_input":chat[count-1]['data'],
                "exp_msg": d['data']
            }
            return content



new = pd.read_parquet('C:\\Users\\Akshay\\PycharmProjects\\chatbot\\saved_data\\df.parquet')

new = new[0:4]
#print("new\n",new)

new["exp_msg"] = new.apply(lambda x: slicing_the_list(x['chat_bot'],x['user_chat'],x['user_id'],x['session_id'],x["date"]),axis=1)

exp_msg_list = new["exp_msg"].tolist()

exp_msg_list = list(filter(None, exp_msg_list))
print("exp_msg_list\n",exp_msg_list)

exit()
