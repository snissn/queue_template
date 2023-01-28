import sys
import time
import datetime

import simplejson as json
from bson.json_util import loads, dumps, JSONOptions
import collections
import pika


import psycopg2
import psycopg2.extras
from psycopg2.extras import Json



class SQL(object):
  def __init__(self, connection_string = None, cursor=False):
    self.keys = None
    self.rows = []
    self.use_cursor = cursor
    self.N = 2000
    self.connection_string = "postgresql:////"
    if connection_string:
      self.connection_string = connection_string

  def write_later(self, data):
    self.rows.append(data)
    if len(self.rows) == self.N:
      self.write_all()

  def write_all(self):
    if self.rows:
      self.write_many(self.tablename, self.rows)
    self.rows = []

  def close(self):
    self.con.close()

  def onconflict(self, keys):
    return ' ON CONFLICT DO NOTHING'

  def build_insert(self, tablename, rowdict_array):
      con = self.connect()
      #keys = map(self.remove_nulls_if_string, rowdict_array[0].keys())# assuming homogenious
      keys = set()
      for row in rowdict_array:
        keys.update(set(row.keys()))
      keys = list(keys)
      columns = ", ".join(keys)
      sql = "insert into %s (%s) values" % ( tablename, columns)
      values_template = "(" + ", ".join(["%s"] * len(keys)) + ")"
      sql += ", ".join([values_template]*len(rowdict_array))
      sql += self.onconflict(keys)
      for rowdict in rowdict_array:
        for key in keys:
          rowdict[key] = rowdict.get(key)

      values = [self.remove_nulls_if_string(item[key]) for item in rowdict_array for key in keys ] #val in item.values() ]
      return sql, values

  def write_many(self, tablename, rowdict_array):
      sql, values = self.build_insert( tablename, rowdict_array)
      self.cursor.execute(sql, values)
      self.con.commit()
