INSERT INTO Tables
VALUES ( 7, "Reserve", 0, "DHF", 3);

INSERT INTO Tables
VALUES ( 8, "Guest", 0, "VF", 3);

INSERT INTO Tables
VALUES ( 9, "Room", 0, "HF", 3);

INSERT INTO Frag_Table 
VALUES ( 19, 'Guest_1', ' guest_id reserve_id name ', 8);

INSERT INTO Frag_Table 
VALUES ( 20, 'Guest_2', ' guest_id name address email phone city ', 8);

INSERT INTO Frag_Table 
VALUES ( 21, 'Guest_3', ' guest_id reserve_id payment ', 8); 

INSERT INTO Frag_Table 
VALUES ( 22, 'Room_1', ' city=Delhi AND price>1000 ', 8);

INSERT INTO Frag_Table 
VALUES ( 23, 'Room_2', ' guest_id name address email phone city ', 8);

INSERT INTO Frag_Table 
VALUES ( 24, 'Room_3', ' guest_id reserve_id payment ', 8); 