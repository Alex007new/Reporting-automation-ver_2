import telegram
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import io
import pandas as pd
import requests
import pandahouse

from datetime import date, timedelta, datetime
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

sns.set()

default_args = {
    'owner': 'a.burlakov-9',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 2, 14)
    }

schedule_interval = '0 11 * * *'

def my_report(chat=None):
    chat_id = chat or -67711XXXXXX
    
    my_token = '6023168328:AAE1WuD5RUDLNDyOgGdJkPBmKXXXXXXXXXXX'
    my_bot = telegram.Bot(token=my_token)
    
    connection = {'host': 'https://clickhouse.lab.karpov.courses',
                  'database':'simulator_20230120',
                  'user':'student',
                  'password':'XXXXXXXXXXXXX'}

    query_1 = """
                SELECT toStartOfDay(toDateTime(date_dt)) AS day,
                       COUNT(DISTINCT l_user_id) AS dau
                FROM
                    (SELECT toDate(l.time) AS date_dt,
                            l.user_id AS l_user_id,
                            r.user_id AS r_user_id
                     FROM simulator_20230120.feed_actions AS l
                         JOIN simulator_20230120.message_actions AS r 
                         ON l.user_id = r.user_id AND toDate(l.time) = toDate(r.time)
                ORDER BY toDate(l.time) DESC) AS t
                WHERE toDate(date_dt) between today()-7 and today()-1
                GROUP BY day
                ORDER BY day DESC
            """
   
    query_2 = """
                SELECT toStartOfDay(toDateTime(date_dt)) AS day,
                       COUNT(DISTINCT l_user_id) AS dau
                FROM
                    (SELECT toDate(l.time) AS date_dt,
                            l.user_id AS l_user_id,
                            r.user_id
                     FROM simulator_20230120.feed_actions AS l
                         LEFT JOIN simulator_20230120.message_actions AS r 
                         ON l.user_id = r.user_id AND toDate(l.time) = toDate(r.time)
                     WHERE r.user_id = 0
                     ORDER BY toDate(l.time) DESC) AS t
                WHERE toDate(date_dt) between today()-7 and today()-1
                GROUP BY day
                ORDER BY day DESC
                """

    query_3 = """
                SELECT toStartOfDay(toDateTime(time)) as day,
                       round(countIf(user_id, action='view') / count(DISTINCT user_id), 1) AS views
                FROM simulator_20230120.feed_actions
                WHERE toDate(time) between today()-7 and today()-1
                GROUP BY day
                ORDER BY day DESC
            """
    
    query_4 = """
                SELECT toStartOfDay(toDateTime(time)) as day,
                       round(countIf(user_id, action='like') / count(DISTINCT user_id), 1) AS likes
                FROM simulator_20230120.feed_actions
                WHERE toDate(time) between today()-7 and today()-1
                GROUP BY day
                ORDER BY day DESC
            """
    
    query_5 = """
                SELECT toStartOfDay(toDateTime(time)) as day,
                       round(countIf(action = 'like') / countIf(action = 'view'), 3) AS ctr
                FROM simulator_20230120.feed_actions
                WHERE toDate(time) between today()-7 and today()-1
                GROUP BY day
                ORDER BY day DESC
            """
    
    query_6 = """
                SELECT toStartOfDay(toDateTime(time)) AS day,
                        round(count(user_id) / count(DISTINCT user_id), 1) AS messages
                FROM simulator_20230120.message_actions
                WHERE toDate(time) between today() - 7 and today() - 1
                GROUP BY day
                ORDER BY day DESC
            """
    
    query_7 = """
                SELECT toMonday(toDateTime(time)) AS week,
                       COUNT(DISTINCT user_id) AS wau
                FROM simulator_20230120.feed_actions
                GROUP BY toMonday(toDateTime(time))
                ORDER BY wau DESC
            """
    
    query_8 = """
                SELECT toStartOfMonth(toDateTime(time)) AS month,
                       count(DISTINCT user_id) AS mau
                FROM simulator_20230120.feed_actions
                GROUP BY toStartOfMonth(toDateTime(time))
                ORDER BY mau DESC
             """
   
    df_q_1 = pandahouse.read_clickhouse(query_1, connection=connection)
    df_q_2 = pandahouse.read_clickhouse(query_2, connection=connection)
    df_q_3 = pandahouse.read_clickhouse(query_3, connection=connection)
    df_q_4 = pandahouse.read_clickhouse(query_4, connection=connection)
    df_q_5 = pandahouse.read_clickhouse(query_5, connection=connection)
    df_q_6 = pandahouse.read_clickhouse(query_6, connection=connection)
    df_q_7 = pandahouse.read_clickhouse(query_7, connection=connection)
    df_q_8 = pandahouse.read_clickhouse(query_8, connection=connection)
    
    msg = "Statistics on services on " + str(date.today()) + ':\n'\
              + '  Used both the news feed and messaging service:' + '\n'\
              + '- yesterday: ' + str(df_q_1.iloc[0, 1]) + '\n' \
              + '- a week ago: ' + str(df_q_1.iloc[6, 1]) + '\n' \
              + '  Used only news feed:' + '\n' \
              + '- yesterday: ' + str(df_q_2.iloc[0, 1]) + '\n' \
              + '- a week ago: ' + str(df_q_2.iloc[6, 1]) + '\n' \
              + '  Number of views per user:' + '\n' \
              + '- yesterday: ' + str(df_q_3.iloc[0, 1]) + '\n' \
              + '- a week ago: ' + str(df_q_3.iloc[6, 1]) + '\n' \
              + '  Number of likes per user:' + '\n' \
              + '- yesterday: ' + str(df_q_4.iloc[0, 1]) + '\n' \
              + '- a week ago: ' + str(df_q_4.iloc[6, 1])  + '\n' \
              + '  CTR: ' + '\n' \
              + '- yesterday: ' + str(df_q_5.iloc[0, 1]) + '\n' \
              + '- a week ago: ' + str(df_q_5.iloc[6, 1]) + '\n' \
              + '  Number of messeges per user:' + '\n' \
              + '- yesterday: ' + str(df_q_6.iloc[0, 1]) + '\n' \
              + '- a week ago: ' + str(df_q_6.iloc[6, 1]) + '\n' \
              + '  WAU: ' + '\n' \
              + '- last thing: ' + str(df_q_7.iloc[0, 1]) + '\n' \
              + '- penultimate: ' + str(df_q_7.iloc[1, 1]) + '\n' \
              + '  MAU: ' + '\n' \
              + '- last thing: ' + str(df_q_8.iloc[0, 1]) + '\n' \
              + '- penultimate: ' + str(df_q_8.iloc[1, 1])  
    
    my_bot.sendMessage(chat_id=chat_id, text=msg)
#######################################################    
    fig, axes = plt.subplots(4, 2, figsize=(14, 20))
        
    fig.suptitle("Statistics on services on", fontsize=18)
    fig.subplots_adjust(hspace=0.9)
    fig.subplots_adjust(wspace=0.4)        

    sns.lineplot(data=df_q_1, x="day", y="dau", ax=axes[0, 0])
    axes[0, 0].set_title("Used both the news feed and messaging service", fontsize=10)
    axes[0, 0].tick_params(axis="x", rotation=45)

    sns.lineplot(data=df_q_2, x="day", y="dau", ax=axes[0, 1])
    axes[0, 1].set_title("Used only news feed", fontsize=10)
    axes[0, 1].tick_params(axis="x", rotation=45)

    sns.lineplot(data=df_q_3, x='day', y="views", ax=axes[1, 0])
    axes[1, 0].set_title("Number of views per user", fontsize=10)
    axes[1, 0].tick_params(axis='x', rotation=45)

    sns.lineplot(data=df_q_4, x='day', y="likes", ax=axes[1, 1])
    axes[1, 1].set_title("Number of likes per user", fontsize=10)
    axes[1, 1].tick_params(axis='x', rotation=45)

    sns.lineplot(data=df_q_5, x='day', y="ctr", ax=axes[2, 0])
    axes[2, 0].set_title("CTR", fontsize=10)
    axes[2, 0].tick_params(axis='x', rotation=45)

    sns.lineplot(data=df_q_6, x="day", y="messages", ax=axes[2, 1])
    axes[2, 1].set_title("Number of messeges per user", fontsize=10)
    axes[2, 1].tick_params(axis='x', rotation=45)

    sns.lineplot(data=df_q_7, x='week', y="wau", ax=axes[3, 0])
    axes[3, 0].set_title("WAU", fontsize=10)
    axes[3, 0].tick_params(axis='x', rotation=45)

    sns.lineplot(data=df_q_8, x='month', y="mau", ax=axes[3, 1])
    axes[3, 1].set_title("MAU", fontsize=10)
    axes[3, 1].tick_params(axis='x', rotation=45)

    plot_object = io.BytesIO()
    plt.savefig(plot_object)

    plot_object.seek(0)
    plot_object.name = ('my_metrics.png')
    plt.close()

    my_bot.sendPhoto(chat_id=chat_id, photo=plot_object)
#######################################################################    
@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def burlakov_dag_task7_2_ver2():
    
    @task()
    def make_report():
        my_report()
    
    make_report()
    
burlakov_dag_task7_2_ver2 = burlakov_dag_task7_2_ver2()   