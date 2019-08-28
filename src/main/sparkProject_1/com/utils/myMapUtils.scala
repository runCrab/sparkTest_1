package com.utils

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}

object myMapUtils {

  /**
    * {"status":"1","regeocode":
    * {"addressComponent":
    * {"city":[],"province":"北京市","adcode":"110108","district":"海淀区","towncode":"110108015000","streetNumber":
    * {"number":"5号","location":"116.310454,39.9927339","direction":"东北","distance":"94.5489","street":"颐和园路"},
    * "country":"中国","township":"燕园街道",
    * "businessAreas":
    * [
    * {"location":"116.303364,39.97641","name":"万泉河","id":"110108"},
    * {"location":"116.314222,39.98249","name":"中关村","id":"110108"},
    * {"location":"116.294214,39.99685","name":"西苑","id":"110108"}
    * ],
    * "building":
    * {"name":"北京大学","type":"科教文化服务;学校;高等院校"},
    * "neighborhood":
    * {"name":"北京大学","type":"科教文化服务;学校;高等院校"},
    * "citycode":"010"},
    * "formatted_address":
    * "北京市海淀区燕园街道北京大学"},
    * "info":"OK","infocode":"10000"}
    */
  def getBussinessFromAmap(long:Double , lat:Double): String ={

    val url = "https://restapi.amap.com/v3/geocode/regeo?location="+long.toString+","+lat+"&key=526cd8b243182f7e00bd2784edcc4a96"
    val jsonstr: String = HttpUtil.get( url )
    val jsonparse = JSON.parseObject( jsonstr )

    val status = jsonparse.getIntValue("status")
    //对status进行非零判断
    if(status == 0) return ""

    val regeocodeJson = jsonparse.getJSONObject("regeocode")
    if(regeocodeJson == null || regeocodeJson.keySet().isEmpty) return ""

    val addressComponent: JSONObject = regeocodeJson.getJSONObject("addressComponent")

    if(addressComponent == null || addressComponent.keySet().isEmpty) return ""
    var businessAreasArr: JSONArray = addressComponent.getJSONArray( "businessAreas" )

    if(businessAreasArr == null || businessAreasArr.isEmpty) return ""

    val buffer = collection.mutable.ListBuffer[String]()

    for(item <- businessAreasArr.toArray){

        val json = item.asInstanceOf[JSONObject]
        buffer.append(json.getString("name"))
      buffer
    }
   buffer.mkString(",")
  }




}
