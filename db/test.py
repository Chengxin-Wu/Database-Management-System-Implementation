import chardet

f = open('../db/db/Course', mode='rb')
data = f.read()
f.close()
print(chardet.detect(data))
print(data)