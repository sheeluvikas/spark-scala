package com.example.etl.app.impl

import com.example.etl.`trait`.ETL

object EtlFactory {

  def apply(): Option[ETL] = {
    val extrator = new AppExtractor
    val loader = new AppLoader
    val transformer = new AppTransformer

    Some(new ETLWrapper(extrator, transformer, loader))
  }

}
