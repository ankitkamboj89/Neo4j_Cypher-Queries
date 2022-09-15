MATCH (n)
DETACH DELETE n;

LOAD CSV WITH HEADERS FROM
'file:///Preprocessed_data2.csv' AS row
//create node
MERGE (jp:Job_posting {id: row.Job_IDs, post_date:datetime(row.post_date)})
MERGE (ty:Job_Type {job_type: row.job_type})
MERGE (comp:Company_Name {company_name: (row.company_name)})
MERGE (tl:Job_Title {job_title: row.job_title})
MERGE (catg:Category {job_category: row.category})
MERGE (c:City {city: row.city})
MERGE (s:State {state: row.refined_state})

// relations
MERGE (jp)-[:posted_by_company]->(comp)
MERGE (jp)-[:has_title]->(tl)
MERGE (jp)-[:belongs_to_category]->(catg)
MERGE (jp)-[:located_in_city]->(c)
MERGE (c)-[:city_located_in_state]->(s)
MERGE (comp)-[:Posting_job_for_category]->(catg)
MERGE (tl)-[:title_is_in_category]->(catg)
MERGE (jp)-[:type_of_job]->(ty);


// Q1 How many jobs are advertised for a given job category in a specified city?
WITH 'Information & Communication Technology' AS category, 'Melbourne' AS city
MATCH (catg:Category)<-[ :belongs_to_category ]-(jp:Job_posting)-[:located_in_city ]->(c:City)
WHERE  c.city = city AND catg.job_category = category
RETURN city, category, COUNT(jp.id) AS job_count;

// Q2 Find job_ids that share the same job_title.

MATCH (tl:Job_Title)<-[:has_title]-(jp:Job_posting)
WITH tl, collect(jp.id) AS job_IDs, COUNT(jp.id) AS Numer_of_jobs_under_title
WHERE size(job_IDs) > 1
RETURN tl.job_title AS job_title,Numer_of_jobs_under_title, job_IDs ;

// Q3 Find all companies that offer jobs in different categories.

MATCH (comp:Company_Name)-[:Posting_job_for_category ]->(catg:Category)
WITH comp, collect(catg.job_category) AS categories, COUNT(catg.job_category) AS Numer_of_categories_advertised_by_company
WHERE size(categories) > 1
RETURN comp.company_name AS company, Numer_of_categories_advertised_by_company, categories;

// Q4 Find jobs based on the presence of a keyword.
WITH ' engineer ' AS keyword
MATCH (catg:Category)<-[:belongs_to_category]-(jp:Job_posting)-[:has_title]->(tl:Job_Title)
WHERE toLower(catg.job_category) CONTAINS keyword OR toLower(tl.job_title) CONTAINS keyword
RETURN keyword, jp.id AS job_id, catg.job_category AS job_category,tl.job_title AS job_title;

// Q5 Find jobs posted during a specified period of time.

WITH '2018-04-13' AS starttime, '2018-04-14' AS endtime
MATCH (jp:Job_posting)-[:posted_by_company]->(comp:Company_Name)
WHERE datetime(starttime) <= jp.post_date AND jp.post_date <= datetime(endtime)
RETURN jp.id AS job_id, jp.post_date AS posting_date;

// Q6 Where is job opening mostly from?
WITH 'Sydney' AS specific_city 
MATCH (ty:Job_Type)<-[ :type_of_job ]-(jp:Job_posting)-[:located_in_city]->(c:City) 
WHERE c.city = specific_city WITH distinct(ty.job_type) AS type_of_job, COUNT(ty.job_type) as number_of_jobs 
RETURN type_of_job,  number_of_jobs;

// Q7 What is the distribution of employment type for a specific job category?
MATCH (jp:Job_posting)-[:located_in_city]->(c:City)-[:city_located_in_state]->(s:State) 
WITH distinct(s.state) AS province_name, COUNT(jp.id) as number_of_jobs 
RETURN province_name, number_of_jobs order by number_of_jobs DESC;