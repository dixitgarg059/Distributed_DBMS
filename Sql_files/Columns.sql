INSERT INTO Columns 
VALUES ( 1, "Roll_No", "INT", "NO", "PRI" );
INSERT INTO Columns 
VALUES ( 1, "Email_Id", "VARCHAR(100)", "NO", "UNI" );
INSERT INTO Columns 
VALUES ( 1, "Phone_No", "VARCHAR(20)", "NO", "NONE" );
INSERT INTO Columns 
VALUES ( 1, "Branch", "enum('CSE','ECE','CSD','CLD','CND','ECD','CHD')", "NO", "NONE" );
INSERT INTO Columns 
VALUES ( 1, "Cgpa", "FLOAT", "NO", "NONE" );
INSERT INTO Columns 
VALUES ( 1, "First_Name", "VARCHAR(100)", "NO", "NONE" );
INSERT INTO Columns 
VALUES ( 1, "Last_Name", "VARCHAR(100)", "NO", "NONE" );

INSERT INTO Columns 
VALUES ( 2, "Faculty_Id", "INT", "NO", "PRI" );
INSERT INTO Columns 
VALUES ( 2, "Email_Id", "VARCHAR(100)", "NO", "UNI" );
INSERT INTO Columns 
VALUES ( 2, "Phone_No", "VARCHAR(20)", "NO", "NONE" );
INSERT INTO Columns 
VALUES ( 2, "Lab", "VARCHAR(20)", "YES", "NONE" );
INSERT INTO Columns 
VALUES ( 2, "First_Name", "VARCHAR(100)", "NO", "NONE" );
INSERT INTO Columns 
VALUES ( 2, "Last_Name", "VARCHAR(100)", "NO", "NONE" );

INSERT INTO Columns 
VALUES ( 3, "Course_Code", "INT", "NO", "PRI" );
INSERT INTO Columns 
VALUES ( 3, "Course_Name", "VARCHAR(100)", "NO", "UNI" );
INSERT INTO Columns 
VALUES ( 3, "Course_Type", "enum('CSE','ECE','HSME')", "NO", "NONE" );
INSERT INTO Columns 
VALUES ( 3, "Credits", "INT", "NO", "NONE" );

INSERT INTO Columns 
VALUES ( 4, "Faculty_Id", "INT", "NO", "PRI" );
INSERT INTO Columns 
VALUES ( 4, "Course_Code", "INT", "NO", "PRI" );

INSERT INTO Columns 
VALUES ( 5, "Roll_No", "INT", "NO", "PRI" );
INSERT INTO Columns 
VALUES ( 5, "Course_Code", "INT", "NO", "PRI" );
INSERT INTO Columns 
VALUES ( 5, "Grade", "INT", "NO", "NONE" );

INSERT INTO Columns 
VALUES ( 6, "Course_Code", "INT", "NO", "PRI" );
INSERT INTO Columns 
VALUES ( 6, "Prerequisites", "VARCHAR(100)", "NO", "PRI" );