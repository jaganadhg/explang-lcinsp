#!/usr/bin/env python
# -*- coding: utf-8 -*-

class Runnable:
    def run(self, input_data=None):
        """
        Core function to execute the task.
        """
        raise NotImplementedError("Subclasses must implement this method.")

    def __or__(self, other):
        """
        Allows chaining tasks using the >> operator.
        """
        return Pipeline(self, other)

class Pipeline(Runnable):
    def __init__(self, *tasks):
        self.tasks = []
        for task in tasks:
            if isinstance(task, Pipeline):
                self.tasks.extend(task.tasks)
            else:
                self.tasks.append(task)

    def __or__(self, other):
        """
        Allows chaining additional tasks to the pipeline.
        """
        return Pipeline(*self.tasks, other)

    def run(self, input_data=None):
        """Executes the tasks in sequence."""
        data = input_data
        for task in self.tasks:
            data = task.run(data)
        return data


