#Variables that can be edited:
    
#Random seed, no random seed = 0
Random_Seed = 37

#Number of iterations to be done
n = 1 

#If street name doesn't exist in the database, query will return empty
StreetName = "Park Oog in Al" 

#Wind direction, 2n is 2km/h with Nortern wind. No wind? use "km"
#Choose from: 2n, 2ne, 2e, 2se, 2s, 2sw, 2w, 2nw
#             6n, 6ne, 6e, 6se, 6s, 6sw, 6w, 6nw
Wind_Direction = "km"  

#Total amount of waypoints to be added to the route
#minimum 2, maximum 8, it however recommended to only use more waypoints with a large buffer
Total_waypoints = 4

#Outer buffer on which random waypoints will be selected (in km)
Outer_buffer_km = 4.5 
#Inner buffer on which random waypoints will be selected (in km)
Inner_buffer_km = 5 


import psycopg2

#connects to database
con = psycopg2.connect(
    host = "localhost",
    dbname = "database",
    user = "username",
    password = "password")
print('Connecting to the PostgreSQL database...')

#connect cursor
cur = con.cursor()

#inner and outer buffer are lon lat degrees
inner_buffer = Outer_buffer_km/111
outer_buffer = Inner_buffer_km/111
Random_Seed_syntax = ", {}" .format(Random_Seed) if Random_Seed > 0 else ""

i = 0
while i < n:

    #drop existing tables
    cur.execute("drop table if exists roads_id")
    cur.execute("drop table if exists donut")
    cur.execute("drop table if exists multipoints")
    cur.execute("drop table if exists points")
    cur.execute("drop table if exists xy_table")
    cur.execute("drop table if exists tsp_route")
    cur.execute("drop table if exists dijkstra_route")
    cur.execute("drop table if exists dijkstra_km")
    cur.execute("drop table if exists cost_matrix_temp")
    cur.execute("drop table if exists total_length_cost")
    
    #commit the transaction
    con.commit()
    
    #create table for waypoints
    cur.execute("create table roads_id (id int, roads_id int)")
    
    con.commit()
    
    #Give starting and ending point
    cur.execute("""insert into roads_id (id, roads_id)
    values (%s, 
    		(select cost_matrix.id 
    		from cost_matrix
    		where cost_matrix.osm_name = '%s'
    		limit 1))"""%(Total_waypoints, StreetName))
    
    con.commit()
    
    #Creates a ring buffer around starting point of 0.04 and 0.044 degrees (lon,lat)
    cur.execute("""create table donut as
    SELECT ST_BuildArea(ST_Collect(smallc,bigc))
    FROM (SELECT
        ST_Buffer(
          ST_AsEWKT((select cost_matrix.geom_way
             from cost_matrix, roads_id
    		 where cost_matrix.id = roads_id.roads_id and roads_id.id = %s))
    , %s) As smallc,
        ST_Buffer(ST_AsEWKT((select cost_matrix.geom_way 
    		 from cost_matrix, roads_id
    		 where cost_matrix.id = roads_id.roads_id and roads_id.id = %s)), %s) As bigc) As foo"""%(Total_waypoints, inner_buffer, Total_waypoints, outer_buffer))
    
    con.commit()
    
    #creates table for random point of type multipoint
    cur.execute ('create table multipoints ("points" varchar, geom geometry)')
    
    con.commit()
    
    #generates random waypoints
    cur.execute ("""insert into multipoints ("points", geom) 
    values (1, 
    ST_GeneratePoints((SELECT 
    ST_AsEWKT(donut.st_buildarea) 
    FROM 
    donut), %s %s))"""%(Total_waypoints -1, Random_Seed_syntax))
        
    con.commit()
    
    #creates table for converting random multipoint feature to single points
    cur.execute ("create table points (id int generated always as identity, geom geometry, x float8, y float8)")
    
    con.commit()
    
    #multipoint to single points
    cur.execute ("""insert into points (geom) 
    SELECT (ST_Dump(ST_AsEWKT(multipoints.geom))).geom AS the_POINT_geom 
    FROM multipoints""")
    
    con.commit()
    
    #creates a table with xy coordinates of points
    cur.execute ('create table xy_table ("id" int generated always as identity, x float, y float)')
    
    con.commit()
    
    #inserts xy coordinates of points
    cur.execute ("""insert into xy_table (x, y) 
    select st_x (ST_GeomFromEWKT(points.geom)), 
    st_y (ST_GeomFromEWKT(points.geom))from points""")
    
    con.commit()
    
    #inserts the road id of the closest road segments to the random points
    #This table already has the starting/ending point
    cur.execute ("""insert into roads_id (id, roads_id) 
    SELECT points.id, 
           (SELECT cost_matrix.id 
            FROM cost_matrix 
            ORDER BY ST_AsEWKT(cost_matrix.geom_way) <-> ST_AsEWKT (points.geom) 
            LIMIT 1 
           ) AS roads_id 
    FROM points""")
    
    con.commit()
    
    #determines the order of point to visit using the travelling salesman problem (TSP)
    #start and end point is 4
    cur.execute ("""create table TSP_route as ( 
    SELECT * FROM pgr_TSP(
        $$ 
        SELECT * FROM pgr_dijkstraCostMatrix(
            'SELECT id, source, target, cost, reverse_cost FROM cost_matrix', 
            (SELECT array_agg(id) 
    		 FROM cost_matrix 
    		 WHERE 
    		 id = (select roads_id.roads_id 
    		  from roads_id where roads_id.id = 1) 
    		 or id = (select roads_id.roads_id 
    		  from roads_id where roads_id.id = 2) 
    		 or id = (select roads_id.roads_id 
    		  from roads_id where roads_id.id = 3) 
    		 or id = (select roads_id.roads_id 
    		  from roads_id where roads_id.id = 4)
             or id = (select roads_id.roads_id 
    		  from roads_id where roads_id.id = 5) 
    		 or id = (select roads_id.roads_id 
    		  from roads_id where roads_id.id = 6) 
    		 or id = (select roads_id.roads_id 
    		  from roads_id where roads_id.id = 7) 
    		 or id = (select roads_id.roads_id 
    		  from roads_id where roads_id.id = 8)), 
            directed := false) 
        $$, 
    	start_id := %s,
        randomize := false))"""%(Total_waypoints))
        
    con.commit()
    
    #creates a temporate copy of cost_matrix
    cur.execute ("create table cost_matrix_temp as table cost_matrix")
    
    con.commit()
    
    #determines the shortest route from the 1st and 2nd point according to the TSP
    cur.execute ("""create table Dijkstra_Route as
    (SELECT * FROM pgr_dijkstra(
        'SELECT id, source, target, %s as cost FROM cost_matrix_temp', 
        (select cost_matrix.source from cost_matrix, tsp_route 
    	 where tsp_route.seq = 1 and cost_matrix.id = tsp_route.node), 
    	(select cost_matrix.target from cost_matrix, tsp_route 
    	 where tsp_route.seq = 2 and cost_matrix.id = tsp_route.node), 
    	FALSE))"""%(Wind_Direction))
        
    con.commit()
    
    #increases the cost of routes already travelled over
    cur.execute ("""update cost_matrix_temp 
    set "cost" = (cost_matrix_temp.cost * 5) 
    where cost_matrix_temp.id in (select dijkstra_route.edge from dijkstra_route)""")
    
    con.commit()
    
    #determines the shortest route from the 2nd and 3rd point according to the TSP
    cur.execute ("""insert into Dijkstra_route
    SELECT * FROM pgr_dijkstra( 
        'SELECT id, source, target, %s as cost FROM cost_matrix_temp', 
        (select cost_matrix.source from cost_matrix, tsp_route 
    	 where tsp_route.seq = 2 and cost_matrix.id = tsp_route.node), 
    	(select cost_matrix.target from cost_matrix, tsp_route 
    	 where tsp_route.seq = 3 and cost_matrix.id = tsp_route.node), 
    	FALSE)"""%(Wind_Direction))
    
    con.commit()
    
    #increases the cost of routes already travelled over
    cur.execute("""update cost_matrix_temp
    set "cost" = (cost_matrix_temp.cost * 5)
    where cost_matrix_temp.id in (select dijkstra_route.edge from dijkstra_route)""")
    
    con.commit()
    
    #determines the shortest route from the 3rd and 4th point according to the TSP
    cur.execute("""insert into Dijkstra_route 
    SELECT * FROM pgr_dijkstra( 
        'SELECT id, source, target, %s as cost FROM cost_matrix_temp', 
        (select cost_matrix.source from cost_matrix, tsp_route 
    	 where tsp_route.seq = 3 and cost_matrix.id = tsp_route.node), 
    	(select cost_matrix.target from cost_matrix, tsp_route 
    	 where tsp_route.seq = 4 and cost_matrix.id = tsp_route.node),
    	FALSE)"""%(Wind_Direction))
        
    con.commit()
    
    #increases the cost of routes already travelled over
    cur.execute("""update cost_matrix_temp 
    set "cost" = (cost_matrix_temp.cost * 5) 
    where cost_matrix_temp.id in (select dijkstra_route.edge from dijkstra_route)""")
    
    con.commit()
    
    #determines the shortest route from the 4th and 5th point according to the TSP
    cur.execute("""insert into Dijkstra_route 
    SELECT * FROM pgr_dijkstra( 
        'SELECT id, source, target, %s as cost FROM cost_matrix_temp', 
        (select cost_matrix.source from cost_matrix, tsp_route 
    	 where tsp_route.seq = 4 and cost_matrix.id = tsp_route.node), 
    	(select cost_matrix.target from cost_matrix, tsp_route 
    	 where tsp_route.seq = 5 and cost_matrix.id = tsp_route.node),
    	FALSE)"""%(Wind_Direction))
        
    con.commit()
    
    
    #increases the cost of routes already travelled over
    cur.execute("""update cost_matrix_temp 
    set "cost" = (cost_matrix_temp.cost * 5) 
    where cost_matrix_temp.id in (select dijkstra_route.edge from dijkstra_route)""")
    
    con.commit()
    
    #determines the shortest route from the 4th and 5th point according to the TSP
    cur.execute("""insert into Dijkstra_route 
    SELECT * FROM pgr_dijkstra( 
        'SELECT id, source, target, %s as cost FROM cost_matrix_temp', 
        (select cost_matrix.source from cost_matrix, tsp_route 
    	 where tsp_route.seq = 5 and cost_matrix.id = tsp_route.node), 
    	(select cost_matrix.target from cost_matrix, tsp_route 
    	 where tsp_route.seq = 6 and cost_matrix.id = tsp_route.node),
    	FALSE)"""%(Wind_Direction))
        
    con.commit()
    
    #increases the cost of routes already travelled over
    cur.execute("""update cost_matrix_temp 
    set "cost" = (cost_matrix_temp.cost * 5) 
    where cost_matrix_temp.id in (select dijkstra_route.edge from dijkstra_route)""")
    
    con.commit()
    
    #determines the shortest route from the 4th and 5th point according to the TSP
    cur.execute("""insert into Dijkstra_route 
    SELECT * FROM pgr_dijkstra( 
        'SELECT id, source, target, %s as cost FROM cost_matrix_temp', 
        (select cost_matrix.source from cost_matrix, tsp_route 
    	 where tsp_route.seq = 6 and cost_matrix.id = tsp_route.node), 
    	(select cost_matrix.target from cost_matrix, tsp_route 
    	 where tsp_route.seq = 7 and cost_matrix.id = tsp_route.node),
    	FALSE)"""%(Wind_Direction))
        
    con.commit()
    
    #increases the cost of routes already travelled over
    cur.execute("""update cost_matrix_temp 
    set "cost" = (cost_matrix_temp.cost * 5) 
    where cost_matrix_temp.id in (select dijkstra_route.edge from dijkstra_route)""")
    
    con.commit()
    
    #determines the shortest route from the 4th and 5th point according to the TSP
    cur.execute("""insert into Dijkstra_route 
    SELECT * FROM pgr_dijkstra( 
        'SELECT id, source, target, %s as cost FROM cost_matrix_temp', 
        (select cost_matrix.source from cost_matrix, tsp_route 
    	 where tsp_route.seq = 7 and cost_matrix.id = tsp_route.node), 
    	(select cost_matrix.target from cost_matrix, tsp_route 
    	 where tsp_route.seq = 8 and cost_matrix.id = tsp_route.node),
    	FALSE)"""%(Wind_Direction))
        
    con.commit()
    
    #creates a table from the calculated route with geometry and lengths
    cur.execute("""create table dijkstra_km as 
    (select dijkstra_route.seq, cost_matrix.id, dijkstra_route.edge, cost_matrix.km, dijkstra_route.cost, cost_matrix.geom_way 
    from dijkstra_route 
    inner join cost_matrix 
    on dijkstra_route.edge=cost_matrix.id 
    order by dijkstra_route.seq) """)
        
    con.commit()
    
    #creates a table for the total length
    cur.execute("create table total_length_cost (length float, cost float, geom geometry)")
    
    #con.commit()
    
    #inserts the total lenght of the whole route to table
    cur.execute("""insert into total_length_cost
                values ((select sum (dijkstra_km.km) from dijkstra_km), 
                (select sum (dijkstra_km.cost) from dijkstra_km), (SELECT ST_Union(ARRAY(SELECT dijkstra_km.geom_way FROM dijkstra_km))))""")
                
    con.commit()
    
    print(i)
      
    i = i + 1
    
else:

    print("Calculation done!")
  
#If the script is terminated early, don't forget to still disconnect from database!

#close the cursor     
cur.close()
#close the connection
con.close()

print("Disconnected from database")
