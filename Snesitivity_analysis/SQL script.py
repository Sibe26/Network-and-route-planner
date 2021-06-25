import psycopg2

#connects to database
con = psycopg2.connect(
    host = "localhost",
    dbname = "ProvUtrecht",
    user = "postgres",
    password = "")
print('Connecting to the PostgreSQL database...')

#connect cursor
cur = con.cursor()

n=101
StreetName = "Park Oog in Al"
Wind_Direction = "km"
inner_buffer = 0.04
outer_buffer = 0.044

i = 101
while i < n:

    #drop existing tables
    cur.execute("drop table roads_id")
    cur.execute("drop table donut")
    cur.execute("drop table multipoints")
    cur.execute("drop table points")
    cur.execute("drop table xy_table")
    cur.execute("drop table tsp_route")
    cur.execute("drop table dijkstra_route")
    cur.execute("drop table dijkstra_km")
    cur.execute("drop table cost_matrix_temp")
    #cur.execute("drop table total_length")
    
    #commit the transaction
    con.commit()
    
    #create table for waypoints
    cur.execute("create table roads_id (id int, roads_id int)")
    
    con.commit()
    
    #Give starting and ending point
    cur.execute("""insert into roads_id (id, roads_id)
    values (4, 
    		(select cost_matrix.id 
    		from cost_matrix
    		where cost_matrix.osm_name = '%s'
    		limit 1))"""%(StreetName))
    
    con.commit()
    
    #Creates a ring buffer around starting point of 0.04 and 0.044 degrees (lon,lat)
    cur.execute("""create table donut as
    SELECT ST_BuildArea(ST_Collect(smallc,bigc))
    FROM (SELECT
        ST_Buffer(
          ST_AsEWKT((select cost_matrix.geom_way
             from cost_matrix, roads_id
    		 where cost_matrix.id = roads_id.roads_id and roads_id.id = 4))
    , %s) As smallc,
        ST_Buffer(ST_AsEWKT((select cost_matrix.geom_way 
    		 from cost_matrix, roads_id
    		 where cost_matrix.id = roads_id.roads_id and roads_id.id = 4)), %s) As bigc) As foo"""%(inner_buffer, outer_buffer))
    
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
    donut), 3, %s))"""%(i))
        
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
    		  from roads_id where roads_id.id = 4)), 
            directed := false) 
        $$, 
    	start_id := 4,
        randomize := false))""")
        
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
    
    #creates a table from the calculated route with geometry and lengths
    cur.execute("""create table dijkstra_km as 
    (select dijkstra_route.seq, cost_matrix.id, dijkstra_route.edge, cost_matrix.km, dijkstra_route.cost, cost_matrix.geom_way 
    from dijkstra_route 
    inner join cost_matrix 
    on dijkstra_route.edge=cost_matrix.id 
    order by dijkstra_route.seq) """)
        
    con.commit()
    
    #creates a table for the total length
    #cur.execute("create table total_length (length float, cost float, geom geometry)")
    
    #con.commit()
    
    #inserts the total lenght of the whole route to table
    cur.execute("""insert into total_length_cost
                values ((select sum (dijkstra_km.km) from dijkstra_km), 
                (select sum (dijkstra_km.cost) from dijkstra_km), (SELECT ST_Union(ARRAY(SELECT dijkstra_km.geom_way FROM dijkstra_km))))""")
                
    con.commit()
    
    print(i)
      
    i = i + 1
    
else:

    print("Calculation without wind done")

StreetName = "Park Oog in Al"
Wind_Direction = "wind_2_n"
inner_buffer = 0.04
outer_buffer = 0.044

#cur.execute("create table total_length_cost_N (length float, cost float, geom geometry)")

i = 1
while i < n:

    #drop existing tables
    cur.execute("drop table roads_id")
    cur.execute("drop table donut")
    cur.execute("drop table multipoints")
    cur.execute("drop table points")
    cur.execute("drop table xy_table")
    cur.execute("drop table tsp_route")
    cur.execute("drop table dijkstra_route")
    cur.execute("drop table dijkstra_km")
    cur.execute("drop table cost_matrix_temp")
    #cur.execute("drop table total_length_cost_N")
    
    #commit the transaction
    con.commit()
    
    #create table for waypoints
    cur.execute("create table roads_id (id int, roads_id int)")
    
    con.commit()
    
    #Give starting and ending point
    cur.execute("""insert into roads_id (id, roads_id)
    values (4, 
    		(select cost_matrix.id 
    		from cost_matrix
    		where cost_matrix.osm_name = '%s'
    		limit 1))"""%(StreetName))
    
    con.commit()
    
    #Creates a ring buffer around starting point of 0.04 and 0.044 degrees (lon,lat)
    cur.execute("""create table donut as
    SELECT ST_BuildArea(ST_Collect(smallc,bigc))
    FROM (SELECT
        ST_Buffer(
          ST_AsEWKT((select cost_matrix.geom_way
             from cost_matrix, roads_id
    		 where cost_matrix.id = roads_id.roads_id and roads_id.id = 4))
    , %s) As smallc,
        ST_Buffer(ST_AsEWKT((select cost_matrix.geom_way 
    		 from cost_matrix, roads_id
    		 where cost_matrix.id = roads_id.roads_id and roads_id.id = 4)), %s) As bigc) As foo"""%(inner_buffer, outer_buffer))
    
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
    donut), 3, %s))"""%(i))
        
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
    		  from roads_id where roads_id.id = 4)), 
            directed := false) 
        $$, 
    	start_id := 4,
        randomize := false))""")
        
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
    
    #creates a table from the calculated route with geometry and lengths
    cur.execute("""create table dijkstra_km as 
    (select dijkstra_route.seq, cost_matrix.id, dijkstra_route.edge, cost_matrix.km, dijkstra_route.cost, cost_matrix.geom_way 
    from dijkstra_route 
    inner join cost_matrix 
    on dijkstra_route.edge=cost_matrix.id 
    order by dijkstra_route.seq) """)
        
    con.commit()
    
    #creates a table for the total length
    #cur.execute("create table total_length_cost_N (length float, cost float, geom geometry)")
    
    #con.commit()
    
    #inserts the total lenght of the whole route to table
    cur.execute("""insert into total_length_cost_2n
                values ((select sum (dijkstra_km.km) from dijkstra_km), 
                (select sum (dijkstra_km.cost) from dijkstra_km), (SELECT ST_Union(ARRAY(SELECT dijkstra_km.geom_way FROM dijkstra_km))))""")
                
    con.commit()
    
    print(i)
      
    i = i + 1
    
else:

    print("Calculation with N wind done")
    


StreetName = "Park Oog in Al"
Wind_Direction = "wind_2_ne"
inner_buffer = 0.04
outer_buffer = 0.044

#cur.execute("create table total_length_cost_NE (length float, cost float, geom geometry)")

i = 1
while i < n:

    #drop existing tables
    cur.execute("drop table roads_id")
    cur.execute("drop table donut")
    cur.execute("drop table multipoints")
    cur.execute("drop table points")
    cur.execute("drop table xy_table")
    cur.execute("drop table tsp_route")
    cur.execute("drop table dijkstra_route")
    cur.execute("drop table dijkstra_km")
    cur.execute("drop table cost_matrix_temp")
    #cur.execute("drop table total_length_cost_N")
    
    #commit the transaction
    con.commit()
    
    #create table for waypoints
    cur.execute("create table roads_id (id int, roads_id int)")
    
    con.commit()
    
    #Give starting and ending point
    cur.execute("""insert into roads_id (id, roads_id)
    values (4, 
    		(select cost_matrix.id 
    		from cost_matrix
    		where cost_matrix.osm_name = '%s'
    		limit 1))"""%(StreetName))
    
    con.commit()
    
    #Creates a ring buffer around starting point of 0.04 and 0.044 degrees (lon,lat)
    cur.execute("""create table donut as
    SELECT ST_BuildArea(ST_Collect(smallc,bigc))
    FROM (SELECT
        ST_Buffer(
          ST_AsEWKT((select cost_matrix.geom_way
             from cost_matrix, roads_id
    		 where cost_matrix.id = roads_id.roads_id and roads_id.id = 4))
    , %s) As smallc,
        ST_Buffer(ST_AsEWKT((select cost_matrix.geom_way 
    		 from cost_matrix, roads_id
    		 where cost_matrix.id = roads_id.roads_id and roads_id.id = 4)), %s) As bigc) As foo"""%(inner_buffer, outer_buffer))
    
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
    donut), 3, %s))"""%(i))
        
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
    		  from roads_id where roads_id.id = 4)), 
            directed := false) 
        $$, 
    	start_id := 4,
        randomize := false))""")
        
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
    
    #creates a table from the calculated route with geometry and lengths
    cur.execute("""create table dijkstra_km as 
    (select dijkstra_route.seq, cost_matrix.id, dijkstra_route.edge, cost_matrix.km, dijkstra_route.cost, cost_matrix.geom_way 
    from dijkstra_route 
    inner join cost_matrix 
    on dijkstra_route.edge=cost_matrix.id 
    order by dijkstra_route.seq) """)
        
    con.commit()
    
    #creates a table for the total length
    #cur.execute("create table total_length_cost_N (length float, cost float, geom geometry)")
    
    #con.commit()
    
    #inserts the total lenght of the whole route to table
    cur.execute("""insert into total_length_cost_2ne
                values ((select sum (dijkstra_km.km) from dijkstra_km), 
                (select sum (dijkstra_km.cost) from dijkstra_km), (SELECT ST_Union(ARRAY(SELECT dijkstra_km.geom_way FROM dijkstra_km))))""")
                
    con.commit()
    
    print(i)
      
    i = i + 1
    
else:

    print("Calculation with NE wind done")
    


StreetName = "Park Oog in Al"
Wind_Direction = "wind_2_e"
inner_buffer = 0.04
outer_buffer = 0.044

#cur.execute("create table total_length_cost_E (length float, cost float, geom geometry)")

i = 1
while i < n:

    #drop existing tables
    cur.execute("drop table roads_id")
    cur.execute("drop table donut")
    cur.execute("drop table multipoints")
    cur.execute("drop table points")
    cur.execute("drop table xy_table")
    cur.execute("drop table tsp_route")
    cur.execute("drop table dijkstra_route")
    cur.execute("drop table dijkstra_km")
    cur.execute("drop table cost_matrix_temp")
    #cur.execute("drop table total_length_cost_N")
    
    #commit the transaction
    con.commit()
    
    #create table for waypoints
    cur.execute("create table roads_id (id int, roads_id int)")
    
    con.commit()
    
    #Give starting and ending point
    cur.execute("""insert into roads_id (id, roads_id)
    values (4, 
    		(select cost_matrix.id 
    		from cost_matrix
    		where cost_matrix.osm_name = '%s'
    		limit 1))"""%(StreetName))
    
    con.commit()
    
    #Creates a ring buffer around starting point of 0.04 and 0.044 degrees (lon,lat)
    cur.execute("""create table donut as
    SELECT ST_BuildArea(ST_Collect(smallc,bigc))
    FROM (SELECT
        ST_Buffer(
          ST_AsEWKT((select cost_matrix.geom_way
             from cost_matrix, roads_id
    		 where cost_matrix.id = roads_id.roads_id and roads_id.id = 4))
    , %s) As smallc,
        ST_Buffer(ST_AsEWKT((select cost_matrix.geom_way 
    		 from cost_matrix, roads_id
    		 where cost_matrix.id = roads_id.roads_id and roads_id.id = 4)), %s) As bigc) As foo"""%(inner_buffer, outer_buffer))
    
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
    donut), 3, %s))"""%(i))
        
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
    		  from roads_id where roads_id.id = 4)), 
            directed := false) 
        $$, 
    	start_id := 4,
        randomize := false))""")
        
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
    
    #creates a table from the calculated route with geometry and lengths
    cur.execute("""create table dijkstra_km as 
    (select dijkstra_route.seq, cost_matrix.id, dijkstra_route.edge, cost_matrix.km, dijkstra_route.cost, cost_matrix.geom_way 
    from dijkstra_route 
    inner join cost_matrix 
    on dijkstra_route.edge=cost_matrix.id 
    order by dijkstra_route.seq) """)
        
    con.commit()
    
    #creates a table for the total length
    #cur.execute("create table total_length_cost_N (length float, cost float, geom geometry)")
    
    #con.commit()
    
    #inserts the total lenght of the whole route to table
    cur.execute("""insert into total_length_cost_2e
                values ((select sum (dijkstra_km.km) from dijkstra_km), 
                (select sum (dijkstra_km.cost) from dijkstra_km), (SELECT ST_Union(ARRAY(SELECT dijkstra_km.geom_way FROM dijkstra_km))))""")
                
    con.commit()
    
    print(i)
      
    i = i + 1
    
else:

    print("Calculation with E wind done")


StreetName = "Park Oog in Al"
Wind_Direction = "wind_2_se"
inner_buffer = 0.04
outer_buffer = 0.044

#cur.execute("create table total_length_cost_SE (length float, cost float, geom geometry)")

i = 1
while i < n:

    #drop existing tables
    cur.execute("drop table roads_id")
    cur.execute("drop table donut")
    cur.execute("drop table multipoints")
    cur.execute("drop table points")
    cur.execute("drop table xy_table")
    cur.execute("drop table tsp_route")
    cur.execute("drop table dijkstra_route")
    cur.execute("drop table dijkstra_km")
    cur.execute("drop table cost_matrix_temp")
    #cur.execute("drop table total_length_cost_N")
    
    #commit the transaction
    con.commit()
    
    #create table for waypoints
    cur.execute("create table roads_id (id int, roads_id int)")
    
    con.commit()
    
    #Give starting and ending point
    cur.execute("""insert into roads_id (id, roads_id)
    values (4, 
    		(select cost_matrix.id 
    		from cost_matrix
    		where cost_matrix.osm_name = '%s'
    		limit 1))"""%(StreetName))
    
    con.commit()
    
    #Creates a ring buffer around starting point of 0.04 and 0.044 degrees (lon,lat)
    cur.execute("""create table donut as
    SELECT ST_BuildArea(ST_Collect(smallc,bigc))
    FROM (SELECT
        ST_Buffer(
          ST_AsEWKT((select cost_matrix.geom_way
             from cost_matrix, roads_id
    		 where cost_matrix.id = roads_id.roads_id and roads_id.id = 4))
    , %s) As smallc,
        ST_Buffer(ST_AsEWKT((select cost_matrix.geom_way 
    		 from cost_matrix, roads_id
    		 where cost_matrix.id = roads_id.roads_id and roads_id.id = 4)), %s) As bigc) As foo"""%(inner_buffer, outer_buffer))
    
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
    donut), 3, %s))"""%(i))
        
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
    		  from roads_id where roads_id.id = 4)), 
            directed := false) 
        $$, 
    	start_id := 4,
        randomize := false))""")
        
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
    
    #creates a table from the calculated route with geometry and lengths
    cur.execute("""create table dijkstra_km as 
    (select dijkstra_route.seq, cost_matrix.id, dijkstra_route.edge, cost_matrix.km, dijkstra_route.cost, cost_matrix.geom_way 
    from dijkstra_route 
    inner join cost_matrix 
    on dijkstra_route.edge=cost_matrix.id 
    order by dijkstra_route.seq) """)
        
    con.commit()
    
    #creates a table for the total length
    #cur.execute("create table total_length_cost_N (length float, cost float, geom geometry)")
    
    #con.commit()
    
    #inserts the total lenght of the whole route to table
    cur.execute("""insert into total_length_cost_2se
                values ((select sum (dijkstra_km.km) from dijkstra_km), 
                (select sum (dijkstra_km.cost) from dijkstra_km), (SELECT ST_Union(ARRAY(SELECT dijkstra_km.geom_way FROM dijkstra_km))))""")
                
    con.commit()
    
    print(i)
      
    i = i + 1
    
else:

    print("Calculation with SE wind done")
    

StreetName = "Park Oog in Al"
Wind_Direction = "wind_2_s"
inner_buffer = 0.04
outer_buffer = 0.044

#cur.execute("create table total_length_cost_S (length float, cost float, geom geometry)")

i = 1
while i < n:

    #drop existing tables
    cur.execute("drop table roads_id")
    cur.execute("drop table donut")
    cur.execute("drop table multipoints")
    cur.execute("drop table points")
    cur.execute("drop table xy_table")
    cur.execute("drop table tsp_route")
    cur.execute("drop table dijkstra_route")
    cur.execute("drop table dijkstra_km")
    cur.execute("drop table cost_matrix_temp")
    #cur.execute("drop table total_length_cost_N")
    
    #commit the transaction
    con.commit()
    
    #create table for waypoints
    cur.execute("create table roads_id (id int, roads_id int)")
    
    con.commit()
    
    #Give starting and ending point
    cur.execute("""insert into roads_id (id, roads_id)
    values (4, 
    		(select cost_matrix.id 
    		from cost_matrix
    		where cost_matrix.osm_name = '%s'
    		limit 1))"""%(StreetName))
    
    con.commit()
    
    #Creates a ring buffer around starting point of 0.04 and 0.044 degrees (lon,lat)
    cur.execute("""create table donut as
    SELECT ST_BuildArea(ST_Collect(smallc,bigc))
    FROM (SELECT
        ST_Buffer(
          ST_AsEWKT((select cost_matrix.geom_way
             from cost_matrix, roads_id
    		 where cost_matrix.id = roads_id.roads_id and roads_id.id = 4))
    , %s) As smallc,
        ST_Buffer(ST_AsEWKT((select cost_matrix.geom_way 
    		 from cost_matrix, roads_id
    		 where cost_matrix.id = roads_id.roads_id and roads_id.id = 4)), %s) As bigc) As foo"""%(inner_buffer, outer_buffer))
    
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
    donut), 3, %s))"""%(i))
        
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
    		  from roads_id where roads_id.id = 4)), 
            directed := false) 
        $$, 
    	start_id := 4,
        randomize := false))""")
        
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
    
    #creates a table from the calculated route with geometry and lengths
    cur.execute("""create table dijkstra_km as 
    (select dijkstra_route.seq, cost_matrix.id, dijkstra_route.edge, cost_matrix.km, dijkstra_route.cost, cost_matrix.geom_way 
    from dijkstra_route 
    inner join cost_matrix 
    on dijkstra_route.edge=cost_matrix.id 
    order by dijkstra_route.seq) """)
        
    con.commit()
    
    #creates a table for the total length
    #cur.execute("create table total_length_cost_N (length float, cost float, geom geometry)")
    
    #con.commit()
    
    #inserts the total lenght of the whole route to table
    cur.execute("""insert into total_length_cost_2S
                values ((select sum (dijkstra_km.km) from dijkstra_km), 
                (select sum (dijkstra_km.cost) from dijkstra_km), (SELECT ST_Union(ARRAY(SELECT dijkstra_km.geom_way FROM dijkstra_km))))""")
                
    con.commit()
    
    print(i)
      
    i = i + 1
    
else:

    print("Calculation with S wind done")
    
StreetName = "Park Oog in Al"
Wind_Direction = "wind_2_sw"
inner_buffer = 0.04
outer_buffer = 0.044

#cur.execute("create table total_length_cost_SW (length float, cost float, geom geometry)")

i = 1
while i < n:

    #drop existing tables
    cur.execute("drop table roads_id")
    cur.execute("drop table donut")
    cur.execute("drop table multipoints")
    cur.execute("drop table points")
    cur.execute("drop table xy_table")
    cur.execute("drop table tsp_route")
    cur.execute("drop table dijkstra_route")
    cur.execute("drop table dijkstra_km")
    cur.execute("drop table cost_matrix_temp")
    #cur.execute("drop table total_length_cost_N")
    
    #commit the transaction
    con.commit()
    
    #create table for waypoints
    cur.execute("create table roads_id (id int, roads_id int)")
    
    con.commit()
    
    #Give starting and ending point
    cur.execute("""insert into roads_id (id, roads_id)
    values (4, 
    		(select cost_matrix.id 
    		from cost_matrix
    		where cost_matrix.osm_name = '%s'
    		limit 1))"""%(StreetName))
    
    con.commit()
    
    #Creates a ring buffer around starting point of 0.04 and 0.044 degrees (lon,lat)
    cur.execute("""create table donut as
    SELECT ST_BuildArea(ST_Collect(smallc,bigc))
    FROM (SELECT
        ST_Buffer(
          ST_AsEWKT((select cost_matrix.geom_way
             from cost_matrix, roads_id
    		 where cost_matrix.id = roads_id.roads_id and roads_id.id = 4))
    , %s) As smallc,
        ST_Buffer(ST_AsEWKT((select cost_matrix.geom_way 
    		 from cost_matrix, roads_id
    		 where cost_matrix.id = roads_id.roads_id and roads_id.id = 4)), %s) As bigc) As foo"""%(inner_buffer, outer_buffer))
    
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
    donut), 3, %s))"""%(i))
        
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
    		  from roads_id where roads_id.id = 4)), 
            directed := false) 
        $$, 
    	start_id := 4,
        randomize := false))""")
        
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
    
    #creates a table from the calculated route with geometry and lengths
    cur.execute("""create table dijkstra_km as 
    (select dijkstra_route.seq, cost_matrix.id, dijkstra_route.edge, cost_matrix.km, dijkstra_route.cost, cost_matrix.geom_way 
    from dijkstra_route 
    inner join cost_matrix 
    on dijkstra_route.edge=cost_matrix.id 
    order by dijkstra_route.seq) """)
        
    con.commit()
    
    #creates a table for the total length
    #cur.execute("create table total_length_cost_N (length float, cost float, geom geometry)")
    
    #con.commit()
    
    #inserts the total lenght of the whole route to table
    cur.execute("""insert into total_length_cost_2SW
                values ((select sum (dijkstra_km.km) from dijkstra_km), 
                (select sum (dijkstra_km.cost) from dijkstra_km), (SELECT ST_Union(ARRAY(SELECT dijkstra_km.geom_way FROM dijkstra_km))))""")
                
    con.commit()
    
    print(i)
      
    i = i + 1
    
else:

    print("Calculation with SW wind done")

StreetName = "Park Oog in Al"
Wind_Direction = "wind_2_w"
inner_buffer = 0.04
outer_buffer = 0.044

#cur.execute("create table total_length_cost_W (length float, cost float, geom geometry)")

i = 1
while i < n:

    #drop existing tables
    cur.execute("drop table roads_id")
    cur.execute("drop table donut")
    cur.execute("drop table multipoints")
    cur.execute("drop table points")
    cur.execute("drop table xy_table")
    cur.execute("drop table tsp_route")
    cur.execute("drop table dijkstra_route")
    cur.execute("drop table dijkstra_km")
    cur.execute("drop table cost_matrix_temp")
    #cur.execute("drop table total_length_cost_N")
    
    #commit the transaction
    con.commit()
    
    #create table for waypoints
    cur.execute("create table roads_id (id int, roads_id int)")
    
    con.commit()
    
    #Give starting and ending point
    cur.execute("""insert into roads_id (id, roads_id)
    values (4, 
    		(select cost_matrix.id 
    		from cost_matrix
    		where cost_matrix.osm_name = '%s'
    		limit 1))"""%(StreetName))
    
    con.commit()
    
    #Creates a ring buffer around starting point of 0.04 and 0.044 degrees (lon,lat)
    cur.execute("""create table donut as
    SELECT ST_BuildArea(ST_Collect(smallc,bigc))
    FROM (SELECT
        ST_Buffer(
          ST_AsEWKT((select cost_matrix.geom_way
             from cost_matrix, roads_id
    		 where cost_matrix.id = roads_id.roads_id and roads_id.id = 4))
    , %s) As smallc,
        ST_Buffer(ST_AsEWKT((select cost_matrix.geom_way 
    		 from cost_matrix, roads_id
    		 where cost_matrix.id = roads_id.roads_id and roads_id.id = 4)), %s) As bigc) As foo"""%(inner_buffer, outer_buffer))
    
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
    donut), 3, %s))"""%(i))
        
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
    		  from roads_id where roads_id.id = 4)), 
            directed := false) 
        $$, 
    	start_id := 4,
        randomize := false))""")
        
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
    
    #creates a table from the calculated route with geometry and lengths
    cur.execute("""create table dijkstra_km as 
    (select dijkstra_route.seq, cost_matrix.id, dijkstra_route.edge, cost_matrix.km, dijkstra_route.cost, cost_matrix.geom_way 
    from dijkstra_route 
    inner join cost_matrix 
    on dijkstra_route.edge=cost_matrix.id 
    order by dijkstra_route.seq) """)
        
    con.commit()
    
    #creates a table for the total length
    #cur.execute("create table total_length_cost_N (length float, cost float, geom geometry)")
    
    #con.commit()
    
    #inserts the total lenght of the whole route to table
    cur.execute("""insert into total_length_cost_2W
                values ((select sum (dijkstra_km.km) from dijkstra_km), 
                (select sum (dijkstra_km.cost) from dijkstra_km), (SELECT ST_Union(ARRAY(SELECT dijkstra_km.geom_way FROM dijkstra_km))))""")
                
    con.commit()
    
    print(i)
      
    i = i + 1
    
else:

    print("Calculation with W wind done")
    
StreetName = "Park Oog in Al"
Wind_Direction = "wind_2_nw"
inner_buffer = 0.04
outer_buffer = 0.044

#cur.execute("create table total_length_cost_NW (length float, cost float, geom geometry)")

i = 1
while i < n:

    #drop existing tables
    cur.execute("drop table roads_id")
    cur.execute("drop table donut")
    cur.execute("drop table multipoints")
    cur.execute("drop table points")
    cur.execute("drop table xy_table")
    cur.execute("drop table tsp_route")
    cur.execute("drop table dijkstra_route")
    cur.execute("drop table dijkstra_km")
    cur.execute("drop table cost_matrix_temp")
    #cur.execute("drop table total_length_cost_N")
    
    #commit the transaction
    con.commit()
    
    #create table for waypoints
    cur.execute("create table roads_id (id int, roads_id int)")
    
    con.commit()
    
    #Give starting and ending point
    cur.execute("""insert into roads_id (id, roads_id)
    values (4, 
    		(select cost_matrix.id 
    		from cost_matrix
    		where cost_matrix.osm_name = '%s'
    		limit 1))"""%(StreetName))
    
    con.commit()
    
    #Creates a ring buffer around starting point of 0.04 and 0.044 degrees (lon,lat)
    cur.execute("""create table donut as
    SELECT ST_BuildArea(ST_Collect(smallc,bigc))
    FROM (SELECT
        ST_Buffer(
          ST_AsEWKT((select cost_matrix.geom_way
             from cost_matrix, roads_id
    		 where cost_matrix.id = roads_id.roads_id and roads_id.id = 4))
    , %s) As smallc,
        ST_Buffer(ST_AsEWKT((select cost_matrix.geom_way 
    		 from cost_matrix, roads_id
    		 where cost_matrix.id = roads_id.roads_id and roads_id.id = 4)), %s) As bigc) As foo"""%(inner_buffer, outer_buffer))
    
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
    donut), 3, %s))"""%(i))
        
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
    		  from roads_id where roads_id.id = 4)), 
            directed := false) 
        $$, 
    	start_id := 4,
        randomize := false))""")
        
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
    
    #creates a table from the calculated route with geometry and lengths
    cur.execute("""create table dijkstra_km as 
    (select dijkstra_route.seq, cost_matrix.id, dijkstra_route.edge, cost_matrix.km, dijkstra_route.cost, cost_matrix.geom_way 
    from dijkstra_route 
    inner join cost_matrix 
    on dijkstra_route.edge=cost_matrix.id 
    order by dijkstra_route.seq) """)
        
    con.commit()
    
    #creates a table for the total length
    #cur.execute("create table total_length_cost_N (length float, cost float, geom geometry)")
    
    #con.commit()
    
    #inserts the total lenght of the whole route to table
    cur.execute("""insert into total_length_cost_2NW
                values ((select sum (dijkstra_km.km) from dijkstra_km), 
                (select sum (dijkstra_km.cost) from dijkstra_km), (SELECT ST_Union(ARRAY(SELECT dijkstra_km.geom_way FROM dijkstra_km))))""")
                
    con.commit()
    
    print(i)
      
    i = i + 1
    
else:

    print("Calculation with NW wind done")
    
    print("All done!")
    #close the cursor     
    cur.close()
    #close the connection
    con.close()
    
    
    
import psycopg2

#connects to database
con = psycopg2.connect(
    host = "localhost",
    dbname = "ProvUtrecht",
    user = "postgres",
    password = "NINyqfXEb12!?")
print('Connecting to the PostgreSQL database...')

#connect cursor
cur = con.cursor()

n=101
StreetName = "Park Oog in Al"
Wind_Direction = "km"
inner_buffer = 0.04
outer_buffer = 0.044

i = 101
while i < n:

    #drop existing tables
    cur.execute("drop table roads_id")
    cur.execute("drop table donut")
    cur.execute("drop table multipoints")
    cur.execute("drop table points")
    cur.execute("drop table xy_table")
    cur.execute("drop table tsp_route")
    cur.execute("drop table dijkstra_route")
    cur.execute("drop table dijkstra_km")
    cur.execute("drop table cost_matrix_temp")
    #cur.execute("drop table total_length")
    
    #commit the transaction
    con.commit()
    
    #create table for waypoints
    cur.execute("create table roads_id (id int, roads_id int)")
    
    con.commit()
    
    #Give starting and ending point
    cur.execute("""insert into roads_id (id, roads_id)
    values (4, 
    		(select cost_matrix.id 
    		from cost_matrix
    		where cost_matrix.osm_name = '%s'
    		limit 1))"""%(StreetName))
    
    con.commit()
    
    #Creates a ring buffer around starting point of 0.04 and 0.044 degrees (lon,lat)
    cur.execute("""create table donut as
    SELECT ST_BuildArea(ST_Collect(smallc,bigc))
    FROM (SELECT
        ST_Buffer(
          ST_AsEWKT((select cost_matrix.geom_way
             from cost_matrix, roads_id
    		 where cost_matrix.id = roads_id.roads_id and roads_id.id = 4))
    , %s) As smallc,
        ST_Buffer(ST_AsEWKT((select cost_matrix.geom_way 
    		 from cost_matrix, roads_id
    		 where cost_matrix.id = roads_id.roads_id and roads_id.id = 4)), %s) As bigc) As foo"""%(inner_buffer, outer_buffer))
    
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
    donut), 3, %s))"""%(i))
        
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
    		  from roads_id where roads_id.id = 4)), 
            directed := false) 
        $$, 
    	start_id := 4,
        randomize := false))""")
        
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
    
    #creates a table from the calculated route with geometry and lengths
    cur.execute("""create table dijkstra_km as 
    (select dijkstra_route.seq, cost_matrix.id, dijkstra_route.edge, cost_matrix.km, dijkstra_route.cost, cost_matrix.geom_way 
    from dijkstra_route 
    inner join cost_matrix 
    on dijkstra_route.edge=cost_matrix.id 
    order by dijkstra_route.seq) """)
        
    con.commit()
    
    #creates a table for the total length
    #cur.execute("create table total_length (length float, cost float, geom geometry)")
    
    #con.commit()
    
    #inserts the total lenght of the whole route to table
    cur.execute("""insert into total_length_cost
                values ((select sum (dijkstra_km.km) from dijkstra_km), 
                (select sum (dijkstra_km.cost) from dijkstra_km), (SELECT ST_Union(ARRAY(SELECT dijkstra_km.geom_way FROM dijkstra_km))))""")
                
    con.commit()
    
    print(i)
      
    i = i + 1
    
else:

    print("Calculation without wind done")

StreetName = "Park Oog in Al"
Wind_Direction = "wind_6_n"
inner_buffer = 0.04
outer_buffer = 0.044

#cur.execute("create table total_length_cost_N (length float, cost float, geom geometry)")

i = 1
while i < n:

    #drop existing tables
    cur.execute("drop table roads_id")
    cur.execute("drop table donut")
    cur.execute("drop table multipoints")
    cur.execute("drop table points")
    cur.execute("drop table xy_table")
    cur.execute("drop table tsp_route")
    cur.execute("drop table dijkstra_route")
    cur.execute("drop table dijkstra_km")
    cur.execute("drop table cost_matrix_temp")
    #cur.execute("drop table total_length_cost_N")
    
    #commit the transaction
    con.commit()
    
    #create table for waypoints
    cur.execute("create table roads_id (id int, roads_id int)")
    
    con.commit()
    
    #Give starting and ending point
    cur.execute("""insert into roads_id (id, roads_id)
    values (4, 
    		(select cost_matrix.id 
    		from cost_matrix
    		where cost_matrix.osm_name = '%s'
    		limit 1))"""%(StreetName))
    
    con.commit()
    
    #Creates a ring buffer around starting point of 0.04 and 0.044 degrees (lon,lat)
    cur.execute("""create table donut as
    SELECT ST_BuildArea(ST_Collect(smallc,bigc))
    FROM (SELECT
        ST_Buffer(
          ST_AsEWKT((select cost_matrix.geom_way
             from cost_matrix, roads_id
    		 where cost_matrix.id = roads_id.roads_id and roads_id.id = 4))
    , %s) As smallc,
        ST_Buffer(ST_AsEWKT((select cost_matrix.geom_way 
    		 from cost_matrix, roads_id
    		 where cost_matrix.id = roads_id.roads_id and roads_id.id = 4)), %s) As bigc) As foo"""%(inner_buffer, outer_buffer))
    
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
    donut), 3, %s))"""%(i))
        
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
    		  from roads_id where roads_id.id = 4)), 
            directed := false) 
        $$, 
    	start_id := 4,
        randomize := false))""")
        
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
    
    #creates a table from the calculated route with geometry and lengths
    cur.execute("""create table dijkstra_km as 
    (select dijkstra_route.seq, cost_matrix.id, dijkstra_route.edge, cost_matrix.km, dijkstra_route.cost, cost_matrix.geom_way 
    from dijkstra_route 
    inner join cost_matrix 
    on dijkstra_route.edge=cost_matrix.id 
    order by dijkstra_route.seq) """)
        
    con.commit()
    
    #creates a table for the total length
    #cur.execute("create table total_length_cost_N (length float, cost float, geom geometry)")
    
    #con.commit()
    
    #inserts the total lenght of the whole route to table
    cur.execute("""insert into total_length_cost_6N
                values ((select sum (dijkstra_km.km) from dijkstra_km), 
                (select sum (dijkstra_km.cost) from dijkstra_km), (SELECT ST_Union(ARRAY(SELECT dijkstra_km.geom_way FROM dijkstra_km))))""")
                
    con.commit()
    
    print(i)
      
    i = i + 1
    
else:

    print("Calculation with N wind done")
    


StreetName = "Park Oog in Al"
Wind_Direction = "wind_6_ne"
inner_buffer = 0.04
outer_buffer = 0.044

#cur.execute("create table total_length_cost_NE (length float, cost float, geom geometry)")

i = 1
while i < n:

    #drop existing tables
    cur.execute("drop table roads_id")
    cur.execute("drop table donut")
    cur.execute("drop table multipoints")
    cur.execute("drop table points")
    cur.execute("drop table xy_table")
    cur.execute("drop table tsp_route")
    cur.execute("drop table dijkstra_route")
    cur.execute("drop table dijkstra_km")
    cur.execute("drop table cost_matrix_temp")
    #cur.execute("drop table total_length_cost_N")
    
    #commit the transaction
    con.commit()
    
    #create table for waypoints
    cur.execute("create table roads_id (id int, roads_id int)")
    
    con.commit()
    
    #Give starting and ending point
    cur.execute("""insert into roads_id (id, roads_id)
    values (4, 
    		(select cost_matrix.id 
    		from cost_matrix
    		where cost_matrix.osm_name = '%s'
    		limit 1))"""%(StreetName))
    
    con.commit()
    
    #Creates a ring buffer around starting point of 0.04 and 0.044 degrees (lon,lat)
    cur.execute("""create table donut as
    SELECT ST_BuildArea(ST_Collect(smallc,bigc))
    FROM (SELECT
        ST_Buffer(
          ST_AsEWKT((select cost_matrix.geom_way
             from cost_matrix, roads_id
    		 where cost_matrix.id = roads_id.roads_id and roads_id.id = 4))
    , %s) As smallc,
        ST_Buffer(ST_AsEWKT((select cost_matrix.geom_way 
    		 from cost_matrix, roads_id
    		 where cost_matrix.id = roads_id.roads_id and roads_id.id = 4)), %s) As bigc) As foo"""%(inner_buffer, outer_buffer))
    
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
    donut), 3, %s))"""%(i))
        
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
    		  from roads_id where roads_id.id = 4)), 
            directed := false) 
        $$, 
    	start_id := 4,
        randomize := false))""")
        
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
    
    #creates a table from the calculated route with geometry and lengths
    cur.execute("""create table dijkstra_km as 
    (select dijkstra_route.seq, cost_matrix.id, dijkstra_route.edge, cost_matrix.km, dijkstra_route.cost, cost_matrix.geom_way 
    from dijkstra_route 
    inner join cost_matrix 
    on dijkstra_route.edge=cost_matrix.id 
    order by dijkstra_route.seq) """)
        
    con.commit()
    
    #creates a table for the total length
    #cur.execute("create table total_length_cost_N (length float, cost float, geom geometry)")
    
    #con.commit()
    
    #inserts the total lenght of the whole route to table
    cur.execute("""insert into total_length_cost_6NE
                values ((select sum (dijkstra_km.km) from dijkstra_km), 
                (select sum (dijkstra_km.cost) from dijkstra_km), (SELECT ST_Union(ARRAY(SELECT dijkstra_km.geom_way FROM dijkstra_km))))""")
                
    con.commit()
    
    print(i)
      
    i = i + 1
    
else:

    print("Calculation with NE wind done")
    


StreetName = "Park Oog in Al"
Wind_Direction = "wind_6_e"
inner_buffer = 0.04
outer_buffer = 0.044

#cur.execute("create table total_length_cost_E (length float, cost float, geom geometry)")

i = 1
while i < n:

    #drop existing tables
    cur.execute("drop table roads_id")
    cur.execute("drop table donut")
    cur.execute("drop table multipoints")
    cur.execute("drop table points")
    cur.execute("drop table xy_table")
    cur.execute("drop table tsp_route")
    cur.execute("drop table dijkstra_route")
    cur.execute("drop table dijkstra_km")
    cur.execute("drop table cost_matrix_temp")
    #cur.execute("drop table total_length_cost_N")
    
    #commit the transaction
    con.commit()
    
    #create table for waypoints
    cur.execute("create table roads_id (id int, roads_id int)")
    
    con.commit()
    
    #Give starting and ending point
    cur.execute("""insert into roads_id (id, roads_id)
    values (4, 
    		(select cost_matrix.id 
    		from cost_matrix
    		where cost_matrix.osm_name = '%s'
    		limit 1))"""%(StreetName))
    
    con.commit()
    
    #Creates a ring buffer around starting point of 0.04 and 0.044 degrees (lon,lat)
    cur.execute("""create table donut as
    SELECT ST_BuildArea(ST_Collect(smallc,bigc))
    FROM (SELECT
        ST_Buffer(
          ST_AsEWKT((select cost_matrix.geom_way
             from cost_matrix, roads_id
    		 where cost_matrix.id = roads_id.roads_id and roads_id.id = 4))
    , %s) As smallc,
        ST_Buffer(ST_AsEWKT((select cost_matrix.geom_way 
    		 from cost_matrix, roads_id
    		 where cost_matrix.id = roads_id.roads_id and roads_id.id = 4)), %s) As bigc) As foo"""%(inner_buffer, outer_buffer))
    
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
    donut), 3, %s))"""%(i))
        
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
    		  from roads_id where roads_id.id = 4)), 
            directed := false) 
        $$, 
    	start_id := 4,
        randomize := false))""")
        
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
    
    #creates a table from the calculated route with geometry and lengths
    cur.execute("""create table dijkstra_km as 
    (select dijkstra_route.seq, cost_matrix.id, dijkstra_route.edge, cost_matrix.km, dijkstra_route.cost, cost_matrix.geom_way 
    from dijkstra_route 
    inner join cost_matrix 
    on dijkstra_route.edge=cost_matrix.id 
    order by dijkstra_route.seq) """)
        
    con.commit()
    
    #creates a table for the total length
    #cur.execute("create table total_length_cost_N (length float, cost float, geom geometry)")
    
    #con.commit()
    
    #inserts the total lenght of the whole route to table
    cur.execute("""insert into total_length_cost_6E
                values ((select sum (dijkstra_km.km) from dijkstra_km), 
                (select sum (dijkstra_km.cost) from dijkstra_km), (SELECT ST_Union(ARRAY(SELECT dijkstra_km.geom_way FROM dijkstra_km))))""")
                
    con.commit()
    
    print(i)
      
    i = i + 1
    
else:

    print("Calculation with E wind done")


StreetName = "Park Oog in Al"
Wind_Direction = "wind_6_se"
inner_buffer = 0.04
outer_buffer = 0.044

#cur.execute("create table total_length_cost_SE (length float, cost float, geom geometry)")

i = 1
while i < n:

    #drop existing tables
    cur.execute("drop table roads_id")
    cur.execute("drop table donut")
    cur.execute("drop table multipoints")
    cur.execute("drop table points")
    cur.execute("drop table xy_table")
    cur.execute("drop table tsp_route")
    cur.execute("drop table dijkstra_route")
    cur.execute("drop table dijkstra_km")
    cur.execute("drop table cost_matrix_temp")
    #cur.execute("drop table total_length_cost_N")
    
    #commit the transaction
    con.commit()
    
    #create table for waypoints
    cur.execute("create table roads_id (id int, roads_id int)")
    
    con.commit()
    
    #Give starting and ending point
    cur.execute("""insert into roads_id (id, roads_id)
    values (4, 
    		(select cost_matrix.id 
    		from cost_matrix
    		where cost_matrix.osm_name = '%s'
    		limit 1))"""%(StreetName))
    
    con.commit()
    
    #Creates a ring buffer around starting point of 0.04 and 0.044 degrees (lon,lat)
    cur.execute("""create table donut as
    SELECT ST_BuildArea(ST_Collect(smallc,bigc))
    FROM (SELECT
        ST_Buffer(
          ST_AsEWKT((select cost_matrix.geom_way
             from cost_matrix, roads_id
    		 where cost_matrix.id = roads_id.roads_id and roads_id.id = 4))
    , %s) As smallc,
        ST_Buffer(ST_AsEWKT((select cost_matrix.geom_way 
    		 from cost_matrix, roads_id
    		 where cost_matrix.id = roads_id.roads_id and roads_id.id = 4)), %s) As bigc) As foo"""%(inner_buffer, outer_buffer))
    
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
    donut), 3, %s))"""%(i))
        
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
    		  from roads_id where roads_id.id = 4)), 
            directed := false) 
        $$, 
    	start_id := 4,
        randomize := false))""")
        
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
    
    #creates a table from the calculated route with geometry and lengths
    cur.execute("""create table dijkstra_km as 
    (select dijkstra_route.seq, cost_matrix.id, dijkstra_route.edge, cost_matrix.km, dijkstra_route.cost, cost_matrix.geom_way 
    from dijkstra_route 
    inner join cost_matrix 
    on dijkstra_route.edge=cost_matrix.id 
    order by dijkstra_route.seq) """)
        
    con.commit()
    
    #creates a table for the total length
    #cur.execute("create table total_length_cost_N (length float, cost float, geom geometry)")
    
    #con.commit()
    
    #inserts the total lenght of the whole route to table
    cur.execute("""insert into total_length_cost_6SE
                values ((select sum (dijkstra_km.km) from dijkstra_km), 
                (select sum (dijkstra_km.cost) from dijkstra_km), (SELECT ST_Union(ARRAY(SELECT dijkstra_km.geom_way FROM dijkstra_km))))""")
                
    con.commit()
    
    print(i)
      
    i = i + 1
    
else:

    print("Calculation with SE wind done")
    

StreetName = "Park Oog in Al"
Wind_Direction = "wind_6_s"
inner_buffer = 0.04
outer_buffer = 0.044

#cur.execute("create table total_length_cost_S (length float, cost float, geom geometry)")

i = 1
while i < n:

    #drop existing tables
    cur.execute("drop table roads_id")
    cur.execute("drop table donut")
    cur.execute("drop table multipoints")
    cur.execute("drop table points")
    cur.execute("drop table xy_table")
    cur.execute("drop table tsp_route")
    cur.execute("drop table dijkstra_route")
    cur.execute("drop table dijkstra_km")
    cur.execute("drop table cost_matrix_temp")
    #cur.execute("drop table total_length_cost_N")
    
    #commit the transaction
    con.commit()
    
    #create table for waypoints
    cur.execute("create table roads_id (id int, roads_id int)")
    
    con.commit()
    
    #Give starting and ending point
    cur.execute("""insert into roads_id (id, roads_id)
    values (4, 
    		(select cost_matrix.id 
    		from cost_matrix
    		where cost_matrix.osm_name = '%s'
    		limit 1))"""%(StreetName))
    
    con.commit()
    
    #Creates a ring buffer around starting point of 0.04 and 0.044 degrees (lon,lat)
    cur.execute("""create table donut as
    SELECT ST_BuildArea(ST_Collect(smallc,bigc))
    FROM (SELECT
        ST_Buffer(
          ST_AsEWKT((select cost_matrix.geom_way
             from cost_matrix, roads_id
    		 where cost_matrix.id = roads_id.roads_id and roads_id.id = 4))
    , %s) As smallc,
        ST_Buffer(ST_AsEWKT((select cost_matrix.geom_way 
    		 from cost_matrix, roads_id
    		 where cost_matrix.id = roads_id.roads_id and roads_id.id = 4)), %s) As bigc) As foo"""%(inner_buffer, outer_buffer))
    
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
    donut), 3, %s))"""%(i))
        
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
    		  from roads_id where roads_id.id = 4)), 
            directed := false) 
        $$, 
    	start_id := 4,
        randomize := false))""")
        
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
    
    #creates a table from the calculated route with geometry and lengths
    cur.execute("""create table dijkstra_km as 
    (select dijkstra_route.seq, cost_matrix.id, dijkstra_route.edge, cost_matrix.km, dijkstra_route.cost, cost_matrix.geom_way 
    from dijkstra_route 
    inner join cost_matrix 
    on dijkstra_route.edge=cost_matrix.id 
    order by dijkstra_route.seq) """)
        
    con.commit()
    
    #creates a table for the total length
    #cur.execute("create table total_length_cost_N (length float, cost float, geom geometry)")
    
    #con.commit()
    
    #inserts the total lenght of the whole route to table
    cur.execute("""insert into total_length_cost_6S
                values ((select sum (dijkstra_km.km) from dijkstra_km), 
                (select sum (dijkstra_km.cost) from dijkstra_km), (SELECT ST_Union(ARRAY(SELECT dijkstra_km.geom_way FROM dijkstra_km))))""")
                
    con.commit()
    
    print(i)
      
    i = i + 1
    
else:

    print("Calculation with S wind done")
    
StreetName = "Park Oog in Al"
Wind_Direction = "wind_6_sw"
inner_buffer = 0.04
outer_buffer = 0.044

#cur.execute("create table total_length_cost_SW (length float, cost float, geom geometry)")

i = 1
while i < n:

    #drop existing tables
    cur.execute("drop table roads_id")
    cur.execute("drop table donut")
    cur.execute("drop table multipoints")
    cur.execute("drop table points")
    cur.execute("drop table xy_table")
    cur.execute("drop table tsp_route")
    cur.execute("drop table dijkstra_route")
    cur.execute("drop table dijkstra_km")
    cur.execute("drop table cost_matrix_temp")
    #cur.execute("drop table total_length_cost_N")
    
    #commit the transaction
    con.commit()
    
    #create table for waypoints
    cur.execute("create table roads_id (id int, roads_id int)")
    
    con.commit()
    
    #Give starting and ending point
    cur.execute("""insert into roads_id (id, roads_id)
    values (4, 
    		(select cost_matrix.id 
    		from cost_matrix
    		where cost_matrix.osm_name = '%s'
    		limit 1))"""%(StreetName))
    
    con.commit()
    
    #Creates a ring buffer around starting point of 0.04 and 0.044 degrees (lon,lat)
    cur.execute("""create table donut as
    SELECT ST_BuildArea(ST_Collect(smallc,bigc))
    FROM (SELECT
        ST_Buffer(
          ST_AsEWKT((select cost_matrix.geom_way
             from cost_matrix, roads_id
    		 where cost_matrix.id = roads_id.roads_id and roads_id.id = 4))
    , %s) As smallc,
        ST_Buffer(ST_AsEWKT((select cost_matrix.geom_way 
    		 from cost_matrix, roads_id
    		 where cost_matrix.id = roads_id.roads_id and roads_id.id = 4)), %s) As bigc) As foo"""%(inner_buffer, outer_buffer))
    
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
    donut), 3, %s))"""%(i))
        
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
    		  from roads_id where roads_id.id = 4)), 
            directed := false) 
        $$, 
    	start_id := 4,
        randomize := false))""")
        
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
    
    #creates a table from the calculated route with geometry and lengths
    cur.execute("""create table dijkstra_km as 
    (select dijkstra_route.seq, cost_matrix.id, dijkstra_route.edge, cost_matrix.km, dijkstra_route.cost, cost_matrix.geom_way 
    from dijkstra_route 
    inner join cost_matrix 
    on dijkstra_route.edge=cost_matrix.id 
    order by dijkstra_route.seq) """)
        
    con.commit()
    
    #creates a table for the total length
    #cur.execute("create table total_length_cost_N (length float, cost float, geom geometry)")
    
    #con.commit()
    
    #inserts the total lenght of the whole route to table
    cur.execute("""insert into total_length_cost_6SW
                values ((select sum (dijkstra_km.km) from dijkstra_km), 
                (select sum (dijkstra_km.cost) from dijkstra_km), (SELECT ST_Union(ARRAY(SELECT dijkstra_km.geom_way FROM dijkstra_km))))""")
                
    con.commit()
    
    print(i)
      
    i = i + 1
    
else:

    print("Calculation with SW wind done")

StreetName = "Park Oog in Al"
Wind_Direction = "wind_6_w"
inner_buffer = 0.04
outer_buffer = 0.044

#cur.execute("create table total_length_cost_W (length float, cost float, geom geometry)")

i = 1
while i < n:

    #drop existing tables
    cur.execute("drop table roads_id")
    cur.execute("drop table donut")
    cur.execute("drop table multipoints")
    cur.execute("drop table points")
    cur.execute("drop table xy_table")
    cur.execute("drop table tsp_route")
    cur.execute("drop table dijkstra_route")
    cur.execute("drop table dijkstra_km")
    cur.execute("drop table cost_matrix_temp")
    #cur.execute("drop table total_length_cost_N")
    
    #commit the transaction
    con.commit()
    
    #create table for waypoints
    cur.execute("create table roads_id (id int, roads_id int)")
    
    con.commit()
    
    #Give starting and ending point
    cur.execute("""insert into roads_id (id, roads_id)
    values (4, 
    		(select cost_matrix.id 
    		from cost_matrix
    		where cost_matrix.osm_name = '%s'
    		limit 1))"""%(StreetName))
    
    con.commit()
    
    #Creates a ring buffer around starting point of 0.04 and 0.044 degrees (lon,lat)
    cur.execute("""create table donut as
    SELECT ST_BuildArea(ST_Collect(smallc,bigc))
    FROM (SELECT
        ST_Buffer(
          ST_AsEWKT((select cost_matrix.geom_way
             from cost_matrix, roads_id
    		 where cost_matrix.id = roads_id.roads_id and roads_id.id = 4))
    , %s) As smallc,
        ST_Buffer(ST_AsEWKT((select cost_matrix.geom_way 
    		 from cost_matrix, roads_id
    		 where cost_matrix.id = roads_id.roads_id and roads_id.id = 4)), %s) As bigc) As foo"""%(inner_buffer, outer_buffer))
    
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
    donut), 3, %s))"""%(i))
        
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
    		  from roads_id where roads_id.id = 4)), 
            directed := false) 
        $$, 
    	start_id := 4,
        randomize := false))""")
        
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
    
    #creates a table from the calculated route with geometry and lengths
    cur.execute("""create table dijkstra_km as 
    (select dijkstra_route.seq, cost_matrix.id, dijkstra_route.edge, cost_matrix.km, dijkstra_route.cost, cost_matrix.geom_way 
    from dijkstra_route 
    inner join cost_matrix 
    on dijkstra_route.edge=cost_matrix.id 
    order by dijkstra_route.seq) """)
        
    con.commit()
    
    #creates a table for the total length
    #cur.execute("create table total_length_cost_N (length float, cost float, geom geometry)")
    
    #con.commit()
    
    #inserts the total lenght of the whole route to table
    cur.execute("""insert into total_length_cost_6W
                values ((select sum (dijkstra_km.km) from dijkstra_km), 
                (select sum (dijkstra_km.cost) from dijkstra_km), (SELECT ST_Union(ARRAY(SELECT dijkstra_km.geom_way FROM dijkstra_km))))""")
                
    con.commit()
    
    print(i)
      
    i = i + 1
    
else:

    print("Calculation with W wind done")
    
StreetName = "Park Oog in Al"
Wind_Direction = "wind_6_nw"
inner_buffer = 0.04
outer_buffer = 0.044

#cur.execute("create table total_length_cost_NW (length float, cost float, geom geometry)")

i = 1
while i < n:

    #drop existing tables
    cur.execute("drop table roads_id")
    cur.execute("drop table donut")
    cur.execute("drop table multipoints")
    cur.execute("drop table points")
    cur.execute("drop table xy_table")
    cur.execute("drop table tsp_route")
    cur.execute("drop table dijkstra_route")
    cur.execute("drop table dijkstra_km")
    cur.execute("drop table cost_matrix_temp")
    #cur.execute("drop table total_length_cost_N")
    
    #commit the transaction
    con.commit()
    
    #create table for waypoints
    cur.execute("create table roads_id (id int, roads_id int)")
    
    con.commit()
    
    #Give starting and ending point
    cur.execute("""insert into roads_id (id, roads_id)
    values (4, 
    		(select cost_matrix.id 
    		from cost_matrix
    		where cost_matrix.osm_name = '%s'
    		limit 1))"""%(StreetName))
    
    con.commit()
    
    #Creates a ring buffer around starting point of 0.04 and 0.044 degrees (lon,lat)
    cur.execute("""create table donut as
    SELECT ST_BuildArea(ST_Collect(smallc,bigc))
    FROM (SELECT
        ST_Buffer(
          ST_AsEWKT((select cost_matrix.geom_way
             from cost_matrix, roads_id
    		 where cost_matrix.id = roads_id.roads_id and roads_id.id = 4))
    , %s) As smallc,
        ST_Buffer(ST_AsEWKT((select cost_matrix.geom_way 
    		 from cost_matrix, roads_id
    		 where cost_matrix.id = roads_id.roads_id and roads_id.id = 4)), %s) As bigc) As foo"""%(inner_buffer, outer_buffer))
    
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
    donut), 3, %s))"""%(i))
        
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
    		  from roads_id where roads_id.id = 4)), 
            directed := false) 
        $$, 
    	start_id := 4,
        randomize := false))""")
        
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
    
    #creates a table from the calculated route with geometry and lengths
    cur.execute("""create table dijkstra_km as 
    (select dijkstra_route.seq, cost_matrix.id, dijkstra_route.edge, cost_matrix.km, dijkstra_route.cost, cost_matrix.geom_way 
    from dijkstra_route 
    inner join cost_matrix 
    on dijkstra_route.edge=cost_matrix.id 
    order by dijkstra_route.seq) """)
        
    con.commit()
    
    #creates a table for the total length
    #cur.execute("create table total_length_cost_N (length float, cost float, geom geometry)")
    
    #con.commit()
    
    #inserts the total lenght of the whole route to table
    cur.execute("""insert into total_length_cost_6NW
                values ((select sum (dijkstra_km.km) from dijkstra_km), 
                (select sum (dijkstra_km.cost) from dijkstra_km), (SELECT ST_Union(ARRAY(SELECT dijkstra_km.geom_way FROM dijkstra_km))))""")
                
    con.commit()
    
    print(i)
      
    i = i + 1
    
else:

    print("Calculation with NW wind done")

    print("All done!")
    #close the cursor     
    cur.close()
    #close the connection
    con.close()