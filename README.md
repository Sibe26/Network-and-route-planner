Dear reader, 

If you are reading this, it probably means that you want to try out our 
application for yourselves. This is possible, but some database preperation 
has to be done. The application can be ran locally and works with 
postgresQL and python. 

(1) First, a database must be created in PGadmin and install the extensions
PostGIS and PGrouting. 

(2) The next step is to open the query tool and 
enter the following query to create a empty cost_matrix table.

Create table cost_matrix (
id int,
osm_id bigint,
osm_name varchar,
osm_meta varchar,
osm_source_id bigint,
osm_target_id bigint,
clazz int,
flags int,
source bigint,
target bigint,
km float,
kmh int,
cost float,
reverse_cost float,
x1 float,
y1 float,
x2 float,
y2 float,
geom_way geometry,
n2 float,
ne2 float,
e2 float,
se2 float,
s2 float,
sw2 float,
w2 float,
nw2 float,
n6 float,
ne6 float,
e6 float,
se6 float,
s6 float,
sw6 float,
w6 float,
nw6 float)


(3.1) Then, the cost_matrix.csv file that was supplied with this readme file is required, it can be accessed here: https://drive.google.com/drive/folders/1k8b_zVp2J5YVnWyL86cYrWrBPrVxkl3A    
It needs to be added to the cost_matrix table. There are two ways to do this. 
Either store the .csv file in a directory that is not your :C drive and 
query the following SQL:

COPY cost_matrix
FROM 'd:\DESTINATION FOLDER.CSV' 
DELIMITER ';' 
CSV HEADER;

or

(3.2) Go to PGAdmin, right click on the cost_matrix table and click import/export.
Switch to import and select the .csv file. Select Header 'yes' and delimitor is ';'.
Then click 'Ok' and the table in postgres and the csv should be merged now.

(4) Congratulations, you are ready to use the python script now! Open it in an 
editor to change the variables and follow the instructions in the python 
script for more information.
