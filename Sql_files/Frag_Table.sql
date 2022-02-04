INSERT INTO Frag_Table 
VALUES ( 1, 'Student1', ' Roll_No Email_Id Phone_No ', 1);
INSERT INTO Frag_Table 
VALUES ( 2, 'Student2', ' Roll_No Branch Cgpa ', 1);
INSERT INTO Frag_Table 
VALUES ( 3, 'Student3', ' Roll_No First_Name Last_Name ', 1);

INSERT INTO Frag_Table 
VALUES ( 4, 'Faculty1', ' Faculty_Id Email_Id ', 2);
INSERT INTO Frag_Table 
VALUES ( 5, 'Faculty2', ' Faculty_Id Phone_No Lab ', 2);
INSERT INTO Frag_Table 
VALUES ( 6, 'Faculty3', ' Faculty_Id First_Name Last_Name ', 2);

INSERT INTO Frag_Table 
VALUES ( 7, 'Course1', ' Course_Type = "CSE" ', 3);
INSERT INTO Frag_Table 
VALUES ( 8, 'Course2', ' Course_Type = "ECE" ', 3);
INSERT INTO Frag_Table 
VALUES ( 9, 'Course3', ' Course_Type = "HSME" ', 3);

INSERT INTO Frag_Table 
VALUES ( 10, 'Teaches1', ' Course.Course_Type(Teaches.Course_Code) = "CSE" ', 4);
INSERT INTO Frag_Table 
VALUES ( 11, 'Teaches2', ' Course.Course_Type(Teaches.Course_Code) = "ECE" ', 4);
INSERT INTO Frag_Table 
VALUES ( 12, 'Teaches3', ' Course.Course_Type(Teaches.Course_Code) = "HSME" ', 4);

INSERT INTO Frag_Table 
VALUES ( 13, 'Opts1', ' Course.Course_Type(Opts.Course_Code) = "CSE" ', 5);
INSERT INTO Frag_Table 
VALUES ( 14, 'Opts2', ' Course.Course_Type(Opts.Course_Code) = "ECE" ', 5);
INSERT INTO Frag_Table 
VALUES ( 15, 'Opts3', ' Course.Course_Type(Opts.Course_Code) = "HSME" ', 5);

INSERT INTO Frag_Table 
VALUES ( 16, 'Course_Prerequisites1', ' Course.Course_Type(Course_Prerequisites.Course_Code) = "CSE" ', 6);
INSERT INTO Frag_Table 
VALUES ( 17, 'Course_Prerequisites2', ' Course.Course_Type(Course_Prerequisites.Course_Code) = "ECE" ', 6);
INSERT INTO Frag_Table 
VALUES ( 18, 'Course_Prerequisites3', ' Course.Course_Type(Course_Prerequisites.Course_Code) = "HSME" ', 6);
