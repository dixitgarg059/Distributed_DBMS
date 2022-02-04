###Assumptions
There will be no nested queries.
The column names after ther where clause in any query will be of the form "Table_Name.Column_Name"
The queries will contain only {'=','>','<','>=','<='} comparision operators.
The queries will contain only "AND" and "OR" Logical operators.
The conditions in the csv file will be in the same format as given in the demo file - 
i) The name of strings and enum will not be in quotes in Horizontal Fragmentation.
ii) The column names in Vertical Fragmentation will be comma separated.
iii) The column names and the fragment name in Derived Horizontal Fragmentation will also be comma separated.
iv) There will be an exactly 3 line gap between both the tables.
v) The column info of the tables of the application is not provided in the csv file. 

###Conventions
The trees are generated using bottom-up approach so the root node will be numbered highest and the leaf nodes will be numbered lowest.
The pair of edges between different nodes is printed below the tree.
The "10.3.5.215" is the Home-Database server. All the codes will be runned on it.

###Run the Code
Change to phase_2 directory.
To populate data using new csv file run command - "python3 add_database.py 'path_of_the_csv_file' ".
Note - if path doesn't exist it will give an error message.
To run the Query processing code - "python3 main.py"
- It will prompt to enter an query, enter the same.

