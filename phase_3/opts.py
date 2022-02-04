import random

cse = []
ece = []
hsme = []
for i in range(1,22):
    k = 0
    l = 0
    m = 0
    j = i
    while k+l+m < 3 or k+l+m > 5:
        k = random.randint(1,3)
        l = random.randint(1,3)
        m = random.randint(1,3)
    cse1 = []
    while len(cse1) < k:
        temp = random.randint(1,6)
        if temp not in cse1:
            cse1.append(temp)
            grade = random.randint(4,10)
            s = '(' + str(j) + ',' + str(temp) + ',' + str(grade) + ')'
            cse.append(s)
    ece1 = []
    while len(ece1) < l:
        temp = random.randint(7,12)
        if temp not in ece1:
            ece1.append(temp)
            grade = random.randint(4,10)
            s = '(' + str(j) + ',' + str(temp) + ',' + str(grade) + ')'
            ece.append(s)
    hsme1 = []
    while len(hsme1) < l:
        temp = random.randint(13,18)
        if temp not in hsme1:
            hsme1.append(temp)
            grade = random.randint(4,10)
            s = '(' + str(j) + ',' + str(temp) + ',' + str(grade) + ')'
            hsme.append(s)

for i in (range(len(cse))):
    print(cse[i],end=",\n")

print()

for i in (range(len(ece))):
    print(ece[i],end=",\n")
print()

for i in (range(len(hsme))):
    print(hsme[i],end=",\n")
print()