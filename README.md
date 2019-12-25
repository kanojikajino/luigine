Luigine
-------

Library for developing an engine using luigi. Key features are:

- auto-naming capability defines the output file name from task parameters.
- `load_output` offers an interface to load the output object from a parental task.
- Automated parameter optimization usign `optuna`

## Dependency
- luigi
- numpy
- optuna
- sklearn

## Installation

```
python setup.py install
```

## Example
See `example.py`.

There are four tasks in `example.py`:

1. `DataPreprocessing` task prepares training and validation data sets.
1. `Train` task, given the result of `DataPreprocessing`, trains a ridge regression model using the training data.
1. `PerformanceEvaluation` task, given the results of the above two tasks, evaluates the model on the validation set.
1. `HyperparameterOptimization` task optimizes the parameters included in `PerformanceEvaluation` task so as to minimize the validation loss.

The following command is used to run `PerformanceEvaluation`:
```
python example.py PerformanceEvaluation --working-dir example_working_dir
```
and the results are stored under `example_working_dir/OUTPUT/eval`.
Log is stored in `ENGLOG/engine.log`

The following command is used to run `HyperparameterOptimization` with 100 trials:
```
python example.py HyperparameterOptimization --working-dir example_working_dir --n-trials 100
```
and the results (sqlite db) are stored under `example_working_dir/OUTPUT/optuna`.
Log is stored in `ENGLOG/engine.log`


## How To Use `AutoNamingTask`

1. All tasks must inherit `AutoNamingTask`, instead of `luigi.Task`.
1. Task dependencies are described in `requires` in the same way as `luigi`, except that `requires` must return a **list** of task instances even if it depends on a single task.
1. Task process should be described in `run_task` (instead of `run`), whose
   - input is `input_list`, a list of output Python objects of dependent tasks, and
   - output is Python objects of this task's computation results.
1. The above Python objects are then processed by `save_output` to be pickled and gzipped.
   - If the user wants to choose other data formats, please implement `save_output` and `load_output`, where `save_output` is used to save the output objects of `run_task`, and `load_output` is used to load the Python objects for further processing.
   - A class variable `output_ext` specifies the file extension.
   - Please set it as `output_ext = luigi.Parameter('[your file extension]')`


## How To Use `OptunaTask`

1. Implement a task that outputs a text file containing a function value (eg, `PerformanceEvaluation` in `example.py`)
   - This task represents the black-box function to be minimized, whose input is a set of task parameters and output is a score to be minimized (in the above example, MSE is the score).
1. Implement a black-box optimization task by inheriting `OptunaTask`, and implement `obj_task`, which returns an instance of the above task.
   - `obj_task` receives a set of task parameters as `kwargs`
   - Task parameters to be tuned are specified in `param.py`. Those who have prefix `@` are tuned by optuna.
   - In `example_working_dir/INPUT/param.py`, `alpha` and `fit_intercept` are tuned.
   - When tuning, the corresponding value must be list of two objects, where
	 - the first one is a string specifying which suggest method in optuna is used (see `suggest_*` methods in https://optuna.readthedocs.io/en/latest/reference/trial.html
	 - the second one specifies the suggest method's inputs
1. The output objects of the optimization task is a tuple of `optuna.study` object and the best set of task parameters.


# Collaborators
- Hiroshi Kajino
- Takeshi Teshima
