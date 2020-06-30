def compareFolders(spark: SparkSession,inputPath1:String,inputPath2:String): List[String] ={ 
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration);
    val sourceFiles = fs.listStatus(new Path(inputPath1)).map(f => f.getPath.toString)
    val targetFiles = fs.listStatus(new Path(inputPath2)).map(f => f.getPath.toString)
    val resutls = sourceFiles diff targetFiles
    println(resutls.mkString(" , "))
    resutls.toList
  }