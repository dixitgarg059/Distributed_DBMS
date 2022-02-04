CREATE TABLE Tables
(
  Table_Id INT NOT NULL,
  Table_Name VARCHAR(200) NOT NULL,
  Columns_No INT NOT NULL,
  Frag_Type enum('VF','DHF','HF') NOT NULL,
  Frags_No INT NOT NULL,
  PRIMARY KEY (Table_Id),
  UNIQUE (Table_Name)
);

CREATE TABLE Sites
(
  Site_Id INT NOT NULL,
  Ip VARCHAR(200) NOT NULL,
  User VARCHAR(200) NOT NULL,
  Password VARCHAR(200) NOT NULL,
  Database_Name VARCHAR(200) NOT NULL,
  Database_Type VARCHAR(200) NOT NULL,
  PRIMARY KEY (Site_Id),
  UNIQUE (Ip)
);

CREATE TABLE Frag_Table
(
  Frag_Id INT NOT NULL,
  Frag_Name VARCHAR(200) NOT NULL,
  Frag_Condition VARCHAR(200) NOT NULL,
  Table_Id INT NOT NULL,
  PRIMARY KEY (Frag_Id),
  FOREIGN KEY (Table_Id) REFERENCES Tables(Table_Id)
);

CREATE TABLE Columns
(
  Table_Id INT NOT NULL,
  Column_Name VARCHAR(200) NOT NULL,
  Data_Type_Col VARCHAR(200) NOT NULL,
  Null_Col enum('NO', 'YES') NOT NULL,
  Key_Col enum('PRI','UNI','NONE'),
  PRIMARY KEY (Column_Name, Table_Id),
  FOREIGN KEY (Table_Id) REFERENCES Tables(Table_Id)
);

CREATE TABLE Allocation
(
  Frag_Id INT NOT NULL,
  Site_Id INT NOT NULL,
  PRIMARY KEY (Frag_Id, Site_Id),
  FOREIGN KEY (Frag_Id) REFERENCES Frag_Table(Frag_Id),
  FOREIGN KEY (Site_Id) REFERENCES Sites(Site_Id)
);