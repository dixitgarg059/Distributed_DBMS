CREATE TABLE Student1
(
  Roll_No INT NOT NULL,
  Email_Id VARCHAR(100) NOT NULL,
  Phone_No VARCHAR(20) NOT NULL,
  PRIMARY KEY (Roll_No),
  UNIQUE (Email_Id)
);

CREATE TABLE Faculty1
(
  Faculty_Id INT NOT NULL,
  Email_Id VARCHAR(100) NOT NULL,
  PRIMARY KEY (Faculty_Id),
  UNIQUE (Email_Id)
);

CREATE TABLE Course1
(
  Course_Code INT NOT NULL,
  Course_Name VARCHAR(100) NOT NULL,
  Course_Type enum('CSE','ECE','HSME') NOT NULL,
  Credits INT NOT NULL,
  PRIMARY KEY (Course_Code),
  UNIQUE (Course_Name)
);

CREATE TABLE Teaches1
(
  Faculty_Id INT NOT NULL,
  Course_Code INT NOT NULL,
  PRIMARY KEY (Faculty_Id, Course_Code),
  FOREIGN KEY (Faculty_Id) REFERENCES Faculty1(Faculty_Id),
  FOREIGN KEY (Course_Code) REFERENCES Course1(Course_Code)
);

CREATE TABLE Opts1
(
  Roll_No INT NOT NULL,
  Course_Code INT NOT NULL,
  Grade INT NOT NULL,
  PRIMARY KEY (Roll_No, Course_Code),
  FOREIGN KEY (Roll_No) REFERENCES Student1(Roll_No),
  FOREIGN KEY (Course_Code) REFERENCES Course1(Course_Code)
);

CREATE TABLE Course_Prerequisites1
(
  Course_Code INT NOT NULL,
  Prerequisites VARCHAR(100) NOT NULL,
  PRIMARY KEY (Prerequisites, Course_Code),
  FOREIGN KEY (Course_Code) REFERENCES Course1(Course_Code)
);

