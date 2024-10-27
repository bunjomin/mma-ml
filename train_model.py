import pandas as pd
import numpy as np
from sklearn.metrics import accuracy_score
import xgboost as xgb
from hyperopt import hp, STATUS_OK, tpe, Trials, fmin

df = pd.read_csv("fighter_stats.csv")

subset = df.loc[(df["date"] > "2015-12-31")]
test_df = subset.loc[(df["date"] > "2022-11-31")]
train_df = subset.drop(test_df.index)
y_test = test_df["outcome"]
X_test = test_df.filter(like="precomp")
print(X_test.shape[0])
y_train = train_df["outcome"]
X_train = train_df.filter(like="precomp")
print(X_train.shape[0])
print(X_test.shape[0] / (X_train.shape[0] + X_test.shape[0]))

base_params = {
    "tree_method": "hist",
    "objective": "binary:logistic",
    "verbosity": 0,
    "n_jobs": -1,
    "n_estimators": 180,
    "seed": 0,
}


def objective(space):
    params = {}
    for k in base_params:
        params[k] = base_params[k]
    for k in space:
        params[k] = space[k]
    for k in ["max_depth", "reg_alpha", "min_child_weight"]:
        params[k] = int(space[k])
    clf = xgb.XGBClassifier(**params)
    evaluation = [(X_train, y_train), (X_test, y_test)]
    clf.fit(X_train, y_train, eval_set=evaluation, verbose=False)
    pred = clf.predict(X_test)
    pred_cast = (pred > 0.5).astype(int)  # Cast to binary
    accuracy = accuracy_score(y_test, pred_cast)
    print("SCORE:", accuracy)
    return {"loss": -accuracy, "status": STATUS_OK}


trials = Trials()

space = {
    "max_depth": hp.quniform("max_depth", 3, 18, 1),
    "gamma": hp.uniform("gamma", 1, 9),
    "reg_alpha": hp.quniform("reg_alpha", 1, 180, 1),
    "reg_lambda": hp.uniform("reg_lambda", 0, 10),
    "min_child_weight": hp.quniform("min_child_weight", 0, 10, 1),
    "learning_rate": hp.uniform("learning_rate", 0.01, 0.2),
    "scale_pos_weight": hp.uniform("scale_pos_weight", 0.1, 1),
    "learning_rate": hp.uniform("learning_rate", 0, 1),
    "colsample_bytree": hp.uniform("colsample_bytree", 0.5, 1),
    "colsample_bylevel": hp.uniform("colsample_bylevel", 0.5, 1),
    "colsample_bynode": hp.uniform("colsample_bynode", 0.5, 1),
    "gamma": hp.uniform("gamma", 0, 10),
    "subsample": hp.uniform("subsample", 0, 1),
}

best_hyperparams = fmin(
    fn=objective, space=space, algo=tpe.suggest, max_evals=100, trials=trials
)

print("The best hyperparameters are : ", "\n")
print(best_hyperparams)

final_params = base_params

for key in best_hyperparams:
    final_params[key] = best_hyperparams[key]

for k in ["max_depth", "reg_alpha", "min_child_weight"]:
    final_params[k] = int(final_params[k])

model = xgb.XGBClassifier(**final_params)  # Create the classifier
model.fit(
    X_train, y_train
)  # Fit the model using the training data; this is the actual training of the model
ypred = model.predict(X_test)  # Predict the test fights using the test fighter's stats
ypred_binary = (ypred >= 0.5).astype(int)  # Cast to binary
accuracy = accuracy_score(y_test, ypred_binary)  # Check the accuracy
print(f"XGBoost test accuracy: {accuracy}")  # Print the accuracy

importances = model.feature_importances_
feature_importances = pd.DataFrame(
    {"Feature": X_train.columns, "Importance": importances}
)
feature_importances = feature_importances.sort_values(by="Importance", ascending=False)
for index, row in feature_importances.head(20).iterrows():
    print(f"{row['Feature']}: {row['Importance']}")

model.save_model("model.json")
