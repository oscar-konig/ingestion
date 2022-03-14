# Databricks notebook source
from datetime import datetime

class NotebookJobResult:
    def __init__(self, job_id, error_flag, message):
        self.job_id = job_id
        self.error_flag = error_flag
        self.message = message
        self.timestamp = datetime.now()
        
    def get_dict(self):
        return {
            "job_id":job_id,
            "error_flag":error_flag,
            "message":message,
            "timesteamp":timestamp,
        }
