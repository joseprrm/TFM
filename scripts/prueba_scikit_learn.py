import sys
from icecream import ic
from client import Client
from sklearn.linear_model import LinearRegression
from sklearn.neighbors import KNeighborsClassifier
import random
import time


client = Client("127.0.0.1", 5000)
ds = client.get_dataset('test_iris_encode')
ds.row_iterator(0, 3)

n = 150
x=ds.prova_scikit_learn(['petal_length', 'petal_width', 'sepal_length', 'petal_width'], 0, n)
y=ds.prova_scikit_learn(['species'], 0, n)
#y = [[e, e] for e in y]
#print(y)

#ic(x)
#ic(y)
#x = [[float(i) for i in l] for l in x]
#y = [int(e) for e in y]


#model = LinearRegression()
from sklearn.cluster import KMeans
from sklearn.ensemble import RandomForestClassifier


#model = KNeighborsClassifier()
#model = KMeans()
#model = LinearRegression()
model = RandomForestClassifier()
model.fit(x, y)
#sys.exit()

predictions = model.predict([x[0]])
print(predictions)
print(int(y[0]))

#import time 
#time.sleep(4)
predictions = model.predict([x[2], x[78], x[100], x[130]])
print(predictions)
print(int(y[2]), int(y[78]), int(y[100]), int(y[130]))
#print(float(y[2]), float(y[78]), float(y[100]), float(y[130]))
#print(y[2], y[78], y[100], y[130])

#predictions = model.predict([x[2], x[78], x[100], x[130]])
#print(predictions)
#print(float(y[2]), float(y[78]), float(y[100]), float(y[130]))
