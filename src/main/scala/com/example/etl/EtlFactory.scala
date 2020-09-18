package com.example.etl

object EtlFactory {

  def apply(): Option[ETL] = {
    val extrator = new AppExtractor
    val loader = new AppLoader
    val transformer = new AppTransformer

    Some(new ETLWrapper(extrator, transformer, loader))
  }

}
