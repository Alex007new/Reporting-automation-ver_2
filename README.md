# Reporting-automation-ver_2

We solve the problem of automating a single report on the operation of the entire application: every morning in Telegram
a single analytical summary will be sent, where there will be information on the news feed,
and messaging service.
>
The report consists of two parts:
  - text with information about the values of key metrics (**DAU, WAU, MAU, CTR, views, likes, number of messages per user**) for the previous day
   and a week ago (in the case of MAU - a month ago);
  - graph with metric values for the previous 7 days;
>  
Automation of sending the report was made using **Airflow**.
>
Fields of the feed_actions table:\
	- user_id\
	- post_id\
	- action\
	- time\
	- gender\
	- age\
	- country\
	- city\
	- os\
	- source\
	- exp_group

Fields of the message_actions table:\
	- user_id\
	- reciever_id\
	- time\
	- source\
	- exp_group\
	- gender\
	- age\
	- country\
	- city\
	- os
