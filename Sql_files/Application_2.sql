CREATE TABLE Student2
(
  Roll_No INT NOT NULL,
  Branch enum('CSE','ECE','CSD','CLD','CND','ECD','CHD') NOT NULL,
  Cgpa FLOAT NOT NULL,
  PRIMARY KEY (Roll_No)
);

CREATE TABLE Faculty2
(
  Faculty_Id INT NOT NULL,
  Phone_No VARCHAR(20) NOT NULL,
  Lab VARCHAR(20),
  PRIMARY KEY (Faculty_Id)
);

CREATE TABLE Course2
(
  Course_Code INT NOT NULL,
  Course_Name VARCHAR(100) NOT NULL,
  Course_Type enum('CSE','ECE','HSME') NOT NULL,
  Credits INT NOT NULL,
  PRIMARY KEY (Course_Code),
  UNIQUE (Course_Name)
);

CREATE TABLE Teaches2
(
  Faculty_Id INT NOT NULL,
  Course_Code INT NOT NULL,
  PRIMARY KEY (Faculty_Id, Course_Code),
  FOREIGN KEY (Faculty_Id) REFERENCES Faculty2(Faculty_Id),
  FOREIGN KEY (Course_Code) REFERENCES Course2(Course_Code)
);

CREATE TABLE Opts2
(
  Roll_No INT NOT NULL,
  Course_Code INT NOT NULL,
  Grade INT NOT NULL,
  PRIMARY KEY (Roll_No, Course_Code),
  FOREIGN KEY (Roll_No) REFERENCES Student2(Roll_No),
  FOREIGN KEY (Course_Code) REFERENCES Course2(Course_Code)
);

CREATE TABLE Course_Prerequisites2
(
  Course_Code INT NOT NULL,
  Prerequisites VARCHAR(100) NOT NULL,
  PRIMARY KEY (Prerequisites, Course_Code),
  FOREIGN KEY (Course_Code) REFERENCES Course2(Course_Code)
);

