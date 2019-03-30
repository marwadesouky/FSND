# "Database code" for the DB Forum.

import datetime
import psycopg2

DBNAME = "news"

statement1 = "What are the most popular three articles of all time?"

query1 = '''select title,count(*) as num from articles,log where
log.path=CONCAT('/article/',articles.slug) group by articles.title 
order by num DESC limit 3;'''

statement2 = "Who are the most popular article authors of all time?"

query2 = '''select authors.name, sum(subq.num) as views from 
authors, (select title, author, count(*) as num from articles,
 log where log.path=CONCAT('/article/',articles.slug) group by 
 articles.title, articles.author order by num DESC) as subq 
 where authors.id=subq.author group by authors.name order by 
 views DESC;'''

statement3 = "On which days did more than 1% of requests lead to errors?"

query3 = '''select * from (select allogs.day,
round(cast((100*error.hits) as numeric) / 
cast(allogs.hits as numeric), 2) as errp from 
(select date(time) as day, count(*) as hits from 
log group by day) as allogs inner join 
(select date(time) as day, count(*) as hits from log
where status not like '200 OK' group by day) 
as error on allogs.day = error.day) as errorp 
where errp > 1.0;'''

def print_query(statement, query, text):
  query_result = exec_query(query)
  print"\n", statement
  for i in query_result:
    print i[0], " -- ", i[1], text
    # print("--")
  # print(query_result)

def exec_query(query):
  db = psycopg2.connect(database=DBNAME)
  c = db.cursor()
  c.execute(query)
  return c.fetchall()
  db.close()


print_query(statement1, query1, " views")
print_query(statement2, query2, " views")
print_query(statement3, query3, " errors")