from client import Client  
from icecream import ic
import sys

from river import compose
from river import linear_model
from river import metrics
from river import preprocessing
from river import neighbors



_client = Client("127.0.0.1", 5000)
ds = _client.get_dataset('iris_test')

X_train = ds.row_iterator(0, 15, ['sepal_length', 'sepal_width', 'petal_length', 'petal_width'])
y_train = ds.row_iterator(0, 15, ['species'])


model = compose.Pipeline(
    preprocessing.StandardScaler(),
#    linear_model.LogisticRegression()
    neighbors.KNNClassifier()
)

metric = metrics.Accuracy()

ic(model.predict_proba_one({}))

sys.exit()
for x, y in zip(X_train, y_train):
    x = dict(x)
    y = y.species
    y_pred = model.predict_one(x)      # make a prediction
    ic(model.predict_proba_one(x))      # make a prediction
    metric.update(y, y_pred)  # update the metric
    model.learn_one(x, y)              # make the model learn
    ic(metric)
