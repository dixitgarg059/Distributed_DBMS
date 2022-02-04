CREATE TABLE Student3
(
  Roll_No INT NOT NULL,
  First_Name VARCHAR(100) NOT NULL,
  Last_Name VARCHAR(100) NOT NULL,
  PRIMARY KEY (Roll_No)
);

CREATE TABLE Faculty3
(
  Faculty_Id INT NOT NULL,
  First_Name VARCHAR(100) NOT NULL,
  Last_Name VARCHAR(100) NOT NULL,
  PRIMARY KEY (Faculty_Id)
);

CREATE TABLE Course3
(
  Course_Code INT NOT NULL,
  Course_Name VARCHAR(100) NOT NULL,
  Course_Type enum('CSE','ECE','HSME') NOT NULL,
  Credits INT NOT NULL,
  PRIMARY KEY (Course_Code),
  UNIQUE (Course_Name)
);

CREATE TABLE Teaches3
(
  Faculty_Id INT NOT NULL,
  Course_Code INT NOT NULL,
  PRIMARY KEY (Faculty_Id, Course_Code),
  FOREIGN KEY (Faculty_Id) REFERENCES Faculty3(Faculty_Id),
  FOREIGN KEY (Course_Code) REFERENCES Course3(Course_Code)
);

CREATE TABLE Opts3
(
  Roll_No INT NOT NULL,
  Course_Code INT NOT NULL,
  Grade INT NOT NULL,
  PRIMARY KEY (Roll_No, Course_Code),
  FOREIGN KEY (Roll_No) REFERENCES Student3(Roll_No),
  FOREIGN KEY (Course_Code) REFERENCES Course3(Course_Code)
);

CREATE TABLE Course_Prerequisites3
(
  Course_Code INT NOT NULL,
  Prerequisites VARCHAR(100) NOT NULL,
  PRIMARY KEY (Prerequisites, Course_Code),
  FOREIGN KEY (Course_Code) REFERENCES Course3(Course_Code)
);

