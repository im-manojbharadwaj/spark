The code contains the function to convert the pyspark dataframe to pyspark structType. The converstion of dataframe to structType is necessary if you want to store the schema in some storage and use that for imposing on other dataframe in future.

The reason for developing this is because if you have the architecture where there is interaction between two different cloud infrastructure then there will always be latency issue or infering schema takes long time. For example, if the data ingestion team is working on collecting the data from android application and storing AWS cloud cluster and your data pipelines are present in Azure then infering schema by hitting the AWS server will take more time. Even for 100K records the latency is found to be 30 - 90 minutes which is very long.

In such scenarios, having two jobs where one inferring schema and another processing the data using the inferred schema will be beneficial. This is the reason, I have developed this function to convert the dataframe to structtype which can be used before imposing the schema on dataframe.

The production flow looks like this:
Data -> (Job to Infer Schema)
			-> (Store the Empty DF with schema in DataLake)
				-> (Job to Read the DF)
					-> (Convert it to StructType)
						-> (Impose on DataFrame)

In this repo you will also see a HTML file which is export of databricks notebook as a HTML format so that you can use it to just import into your databricks workspace.