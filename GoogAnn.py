# -*- coding: utf-8 -*-
"""
Created on Wed Sep 12 15:47:21 2018

@author: glenn.shooter
"""


import argparse
from apiclient.discovery import build
import httplib2
from oauth2client import client
from oauth2client import file
from oauth2client import tools
import pandas as pd
from oauth2client.service_account import ServiceAccountCredentials


class goog_ann:
    
    def __init__(self, view_id, dimensions, metrics, start_date, end_date,
                 scope = ['https://www.googleapis.com/auth/analytics.readonly'],
                 discovery_uri = ('https://analyticsreporting.googleapis.com/$discovery/rest'),
                 client_secrets_path = r'C:/Users/glenn.shooter/Documents/Keyword Research Automation/keyword-automation.json' ):
        
        self.view_id = view_id #view id for GA view you wish to query
        self.dimensions = dimensions #Enter all dimensions in your query (each list of dimesnisions should start with "name")
        self.metrics = metrics #Enter all metrics in your query (each list of dimesnisions should start with "expression") 
        self.start_date = start_date #form year-month-day
        self.end_date = end_date # form year-month-day
        
        self.scope = scope
        self.discovery_uri = discovery_uri
        self.client_secrets_path = client_secrets_path  #This needs to be relevant to your own computer
        self.analytics = []
        self.qry = []
        
    def authorise(self):
        """Method authorises the google analytics object"""
        
        parser = argparse.ArgumentParser(
                formatter_class=argparse.RawDescriptionHelpFormatter,
                parents=[tools.argparser])
        flags = parser.parse_args([])
        # Set up a Flow object to be used if we need to authenticate.
        flow = client.flow_from_clientsecrets(
              self.client_secrets_path, scope=self.scope,
              message=tools.message_if_missing(self.client_secrets_path))

        # Prepare credentials, and authorize HTTP object with them.
        # If the credentials don't exist or are invalid run through the native client
        # flow. The Storage object will ensure that if successful the good
        # credentials will get written back to a file.
        storage = file.Storage('analyticsreporting.dat')
        credentials = storage.get()
        if credentials is None or credentials.invalid:
            credentials = tools.run_flow(flow, storage, flags)
        http = credentials.authorize(http=httplib2.Http())
          
        # Build the service object.
        analytics = build('analytics', 'v4', http=http, discoveryServiceUrl=self.discovery_uri)
          
        self.analytics = analytics
        
    def get_service(self, api_name = 'analytics', api_version =  "v4" ):
        """Get a service that communicates to a Google API.

        Args:
            api_name: The name of the api to connect to.
            api_version: The api version to connect to.
            scopes: A list auth scopes to authorize for the application.
            key_file_location: The path to a valid service account JSON key file.

        Returns:
            A service that is connected to the specified API.
        """

        credentials = ServiceAccountCredentials.from_json_keyfile_name(
            self.client_secrets_path , scopes=self.scope)

        # Build the service object.
        service = build(api_name, api_version, credentials=credentials)
    
        self.analytics = service
    
    def query(self):
        # Use the Analytics Service Object to query the Analytics Reporting API V4.
      metrics = self.metrics
      dimensions = self.dimensions
      
      if len(metrics) == 2: #if there is one metric in the query
          name = metrics[0]
          metric_query = metrics[1]
      if len(metrics) == 3: #if there are two metrics in the query
          name = metrics[0]
          metric_query_one = metrics[1]
          metric_query_two = metrics[2]      
      if len(metrics) == 4: #if there are three metrics in the query
          name = metrics[0]
          metric_query_one = metrics[1]
          metric_query_two = metrics[2]
          metric_query_three = metrics[3]
      if len(metrics) == 5: #if there are four metrics in the query
          name = metrics[0]
          metric_query_one = metrics[1]
          metric_query_two = metrics[2]
          metric_query_three = metrics[3]
          metric_query_four = metrics[4]
      if len (metrics) == 6: #if there are five metrics in the query
          name = metrics[0]
          metric_query_one = metrics[1]
          metric_query_two = metrics[2]
          metric_query_three = metrics[3]
          metric_query_four = metrics[4]
          metric_query_five = metrics[5]
          
      if len(dimensions) == 2: #if there is one dimension in the query
          d_name = dimensions[0]
          dimension = dimensions[1]
      if len(dimensions) == 3: #if there are two dimensions in the query
          d_name = dimensions[0]
          dimension_one = dimensions[1]
          dimension_two = dimensions[2]
      if len(dimensions) == 4: #if there are three dimensions in the query 
          d_name = dimensions[0]
          dimension_one = dimensions[1]
          dimension_two = dimensions[2]
          dimension_three = dimensions[3]

      
      if len(metrics) == 2: #if the query contains 1 metric and 1 dimension
          if len(dimensions) == 2:
              qry = self.analytics.reports().batchGet(
              body={
                'reportRequests': [
                {
                  'viewId': self.view_id,
                  'dateRanges': [{'startDate': self.start_date, 'endDate': self.end_date}],
                  'metrics': [{name : metric_query}],
                  'dimensions': [{d_name : dimension}]
                }]
              }
          ).execute()
    
      if len(metrics) == 2: #if the query contains 1 metric and 1 dimension
          if len(dimensions) == 3:
            qry = self.analytics.reports().batchGet(
              body={
                'reportRequests': [
                {
                  'viewId': self.view_id,
                  'dateRanges': [{'startDate': self.start_date, 'endDate': self.end_date}],
                  'metrics': [{name : metric_query}],
                  'dimensions': [{d_name : dimension_one},{d_name : dimension_two}]
                }]
              }
          ).execute()
    
      if len(metrics) == 2: #if the query contains 1 metric and 1 dimension
          if len(dimensions) == 4:
            qry = self.analytics.reports().batchGet(
              body={
                'reportRequests': [
                {
                  'viewId': self.view_id,
                  'dateRanges': [{'startDate': self.start_date, 'endDate': self.end_date}],
                  'metrics': [{name : metric_query}],
                  'dimensions': [{d_name : dimension_one},{d_name : dimension_two},{d_name : dimension_three}]
                }]
              }
          ).execute()
    
      if len(metrics) == 3: #if the query contains 2 metrics and 1 dimension
          if len(dimensions) == 2:
              qry = self.analytics.reports().batchGet(
                body={
                'reportRequests': [
                {
                  'viewId': self.view_id,
                  'dateRanges': [{'startDate': self.start_date, 'endDate': self.end_date}],
                  'metrics': [{name : metric_query_one},{name : metric_query_two}],
                  'dimensions': [{d_name : dimension}]
                }]
              }
          ).execute()
        
      if len(metrics) == 3: #if the query contains 2 metrics and 2 dimensions
          if len(dimensions) == 3:
              qry = self.analytics.reports().batchGet(
                body={
                'reportRequests': [
                {
                  'viewId': self.view_id,
                  'dateRanges': [{'startDate': self.start_date, 'endDate': self.end_date}],
                  'metrics': [{name : metric_query_one},{name : metric_query_two}],
                  'dimensions': [{d_name : dimension_one},{d_name : dimension_two}]
                }]
              }
          ).execute()
        
      if len(metrics) == 3: #if the query contains 2 metrics and 2 dimensions
          if len(dimensions) == 4:
              qry = self.analytics.reports().batchGet(
                body={
                'reportRequests': [
                {
                  'viewId': self.view_id,
                  'dateRanges': [{'startDate': self.start_date, 'endDate': self.end_date}],
                  'metrics': [{name : metric_query_one},{name : metric_query_two}],
                  'dimensions': [{d_name : dimension_one},{d_name : dimension_two},{d_name : dimension_three}]
                }]
              }
          ).execute()
        
    
      if len(metrics) == 4: #if the query contains 3 metrics and 1 dimension
          if len(dimensions) == 2:
              qry = self.analytics.reports().batchGet(
                body={
                'reportRequests': [
                {
                  'viewId': self.view_id,
                  'dateRanges': [{'startDate': self.start_date, 'endDate': self.end_date}],
                  'metrics': [{name : metric_query_one},{name : metric_query_two},{name : metric_query_three}],
                  'dimensions': [{d_name : dimension}]
                }]
              }
          ).execute()
        
      if len(metrics) == 4: #if the query contains 3 metrics and 2 dimensions
          if len(dimensions) == 3:
              qry = self.analytics.reports().batchGet(
                body={
                'reportRequests': [
                {
                  'viewId': self.view_id,
                  'dateRanges': [{'startDate': self.start_date, 'endDate': self.end_date}],
                  'metrics': [{name : metric_query_one},{name : metric_query_two},{name : metric_query_three}],
                  'dimensions': [{d_name : dimension_one},{d_name : dimension_two}]
                }]
              }
          ).execute()
        
      if len(metrics) == 4: #if the query contains 3 metrics and 2 dimensions
          if len(dimensions) == 4:
              qry = self.analytics.reports().batchGet(
                body={
                'reportRequests': [
                {
                  'viewId': self.view_id,
                  'dateRanges': [{'startDate': self.start_date, 'endDate': self.end_date}],
                  'metrics': [{name : metric_query_one},{name : metric_query_two},{name : metric_query_three}],
                  'dimensions': [{d_name : dimension_one},{d_name : dimension_two},{d_name : dimension_three}]
                }]
              }
          ).execute()
    
      if len(metrics) == 5: #if the query contains 4 metrics and 1 dimension
          if len(dimensions) == 2:
              qry = self.analytics.reports().batchGet(
                body={
                'reportRequests': [
                {
                  'viewId': self.view_id,
                  'dateRanges': [{'startDate': self.start_date, 'endDate': self.end_date}],
                  'metrics': [{name : metric_query_one},{name : metric_query_two},{name : metric_query_three},{name : metric_query_four}],
                  'dimensions': [{d_name : dimension}]
                }]
              }
          ).execute()
        
      if len(metrics) == 5: #if the query contains 4 metrics and 2 dimensions
          if len(dimensions) == 3:
              qry = self.analytics.reports().batchGet(
                body={
                'reportRequests': [
                {
                  'viewId': self.view_id,
                  'dateRanges': [{'startDate': self.start_date, 'endDate': self.end_date}],
                  'metrics': [{name : metric_query_one},{name : metric_query_two},{name : metric_query_three},{name : metric_query_four}],
                  'dimensions': [{d_name : dimension_one},{d_name : dimension_two}]
                }]
              }
          ).execute()
        
      if len(metrics) == 5: #if the query contains 4 metrics and 2 dimensions
          if len(dimensions) == 4:
              qry = self.analytics.reports().batchGet(
                body={
                'reportRequests': [
                {
                  'viewId': self.view_id,
                  'dateRanges': [{'startDate': self.start_date, 'endDate': self.end_date}],
                  'metrics': [{name : metric_query_one},{name : metric_query_two},{name : metric_query_three},{name : metric_query_four}],
                  'dimensions': [{d_name : dimension_one},{d_name : dimension_two},{d_name : dimension_three}]
                }]
              }
          ).execute()
        
      if len(metrics) == 6: #if the query contains 5 metrics and 1 dimension
          if len(dimensions) == 2:
              qry = self.analytics.reports().batchGet(
                body={
                'reportRequests': [
                {
                  'viewId': self.view_id,
                  'dateRanges': [{'startDate': self.view_id, 'endDate': self.end_date}],
                  'metrics': [{name : metric_query_one},{name : metric_query_two},{name : metric_query_three},{name : metric_query_four}, {name : metric_query_five}],
                  'dimensions': [{d_name : dimension}]
                }]
              }
          ).execute()
        
      if len(metrics) == 6: #if the query contains 5 metrics and 2 dimensions
          if len(dimensions) == 3:
              qry = self.analytics.reports().batchGet(
                body={
                'reportRequests': [
                {
                  'viewId': self.view_id,
                  'dateRanges': [{'startDate': self.start_date, 'endDate': self.end_date}],
                  'metrics': [{name : metric_query_one},{name : metric_query_two},{name : metric_query_three},{name : metric_query_four}, {name : metric_query_five}],
                  'dimensions': [{d_name : dimension_one},{d_name : dimension_two}]
                }]
              }
          ).execute()
        
      if len(metrics) == 6: #if the query contains 5 metrics and 2 dimensions
          if len(dimensions) == 4:
              qry = self.analytics.reports().batchGet(
                body={
                'reportRequests': [
                {
                  'viewId': self.view_id,
                  'dateRanges': [{'startDate': self.start_date, 'endDate': self.end_date}],
                  'metrics': [{name : metric_query_one},{name : metric_query_two},{name : metric_query_three},{name : metric_query_four}, {name : metric_query_five}],
                  'dimensions': [{d_name : dimension_one},{d_name : dimension_two},{d_name : dimension_three}]
                }]
              }
          ).execute()
      self.qry = qry
      

 
    

