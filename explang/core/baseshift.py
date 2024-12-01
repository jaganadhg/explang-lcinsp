#!/usr/bin/env python
# -*- coding: utf-8 -*-

class SRunnable:
    def run(self, input_data=None):
        """
        Core function to execute the task.
        """
        raise NotImplementedError("Subclasses must implement this method.")

    def __rshift__(self, other):
        """
        Allows chaining tasks using the >> operator.
        """
        return SPipeline(self, other)

class SPipeline(SRunnable):
    def __init__(self, *tasks):
        self.tasks = []
        for task in tasks:
            if isinstance(task, SPipeline):
                self.tasks.extend(task.tasks)
            else:
                self.tasks.append(task)

    def __rshift__(self, other):
        """
        Allows chaining additional tasks to the pipeline.
        """
        return SPipeline(*self.tasks, other)

    def run(self, input_data=None):
        """Executes the tasks in sequence."""
        data = input_data
        for task in self.tasks:
            data = task.run(data)
        return data


