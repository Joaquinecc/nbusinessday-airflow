from typing import Optional
from pendulum import Date, DateTime, Time, timezone
from datetime import date
import pandas as pd
from airflow.plugins_manager import AirflowPlugin
from airflow.timetables.base import DagRunInfo, DataInterval, TimeRestriction, Timetable
import os
UTC = timezone("America/Asuncion")

def n_dia_habil(n:int,month:int,year:int,time:Time=Time.min) ->DateTime:
    """Calculate the n business day of the selected month and year. 
    Count the days in the month that are not weekends or holidays until it reach n.

    Args:
        n (int): n business day
        month (int): month
        year (int): year
        time (Time, optional): time of the return date. Defaults to Time.min.

    Returns:
        DateTime: the n business day of the selected month of that year
    """
    if month == 13:
        month=1
        year+=1
    elif month==0:
        month=12
        year-=1  
    day=1
    #Get list of holidays in datetime.date format
    holidays=pd.to_datetime(pd.read_csv("plugins/feriados.csv",sep='\t').NOMBRE).dt.date
    #remove unnecesary holidays
    minimum_day=date(year,month,1)
    holidays=holidays[holidays>=minimum_day]
    while(n>0):
        date_t=Date(year,month,day)
        day+=1
        #if sunday or saturday, skip and not count
        if date_t.weekday() in (5,6):
            continue
        # if is a holiday, skip and not count. To be able to compare, date_t is cast to datetime.date
        if holidays.isin([ date.fromisoformat(date_t.to_date_string())]).any():
            continue 
        n-=1 
    return DateTime.combine(date_t,time).replace(tzinfo=UTC)
class NBusinessDay(Timetable):
    def __init__(self, n_day: int,schedule_at: Time):
        self.n_day=n_day
        self._schedule_at = schedule_at

    def serialize(self):
        return {"n_day": str(self.n_day),"schedule_at": self._schedule_at.isoformat()}

    @classmethod
    def deserialize(cls, value):
        return cls(int(value["n_day"]),Time.fromisoformat(value["schedule_at"]))   
        
    def infer_manual_data_interval(self, run_after: DateTime) -> DataInterval:
        print("pwd ",os.getcwd())
        print("ls ",os.listdir("plugins/"))
        n_bussiness_day_current_month=n_dia_habil(self.n_day,run_after.month,run_after.year,self._schedule_at)
        print("n_bussiness_day_current_month ", n_bussiness_day_current_month)
        if run_after< n_bussiness_day_current_month:
            start=n_dia_habil(self.n_day,run_after.month-1,run_after.year,self._schedule_at)
            end=n_bussiness_day_current_month
        else:
            start=n_bussiness_day_current_month
            end=n_dia_habil(self.n_day,run_after.month+1,run_after.year,self._schedule_at)
        return DataInterval(start=start, end=end)
  
    def next_dagrun_info(
        self,
        *,
        last_automated_data_interval: Optional[DataInterval],
        restriction: TimeRestriction,
    ) -> Optional[DagRunInfo]:
        if last_automated_data_interval is not None:  # There was a previous run on the regular schedule.
            last_end = last_automated_data_interval.end
            print("last_end : ",last_end)
            n_bussiness_day_current_month=n_dia_habil(self.n_day,last_end.month,last_end.year,self._schedule_at)
            if last_end<n_bussiness_day_current_month : # If previous run was already pass the n business day of current month
                next_start = n_dia_habil(self.n_day,last_end.month-1,last_end.year,self._schedule_at)
                next_end = n_bussiness_day_current_month
            else: # If previous period started at 16:30, next period will start at 6:00 next day and end at 16:30
                next_start = n_bussiness_day_current_month
                next_end = n_dia_habil(self.n_day,last_end.month+1,last_end.year,self._schedule_at)
        else:  # This is the first ever run on the regular schedule.
            next_start = restriction.earliest
            if next_start is None:  # No start_date. Don't schedule.
                return None
            if not restriction.catchup: # If the DAG has catchup=False, today is the earliest to consider.
                today= DateTime.today()
                n_bussiness_day_current_month=n_dia_habil(self.n_day,today.month,today.year,self._schedule_at)
                if today < n_bussiness_day_current_month:
                    next_start = n_bussiness_day_current_month=n_dia_habil(self.n_day,today.month-1,today.year,self._schedule_at)
                    next_end=n_bussiness_day_current_month
                else:
                    next_start = n_bussiness_day_current_month
                    next_end = n_dia_habil(self.n_day,today.month+1,today.year,self._schedule_at)
        if restriction.latest is not None and next_start > restriction.latest:
            return None  # Over the DAG's scheduled end; don't schedule.
        return DagRunInfo.interval(start=next_start, end=next_end)


class NBusinessTimetablePlugin(AirflowPlugin):
    name = "nbusiness_timetable_plugin"
    timetables = [NBusinessDay]

