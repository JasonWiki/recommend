package com.angejia.dw.common.util

import scala.util.parsing.json.JSON

// play Json
import play.api.libs.json._
import play.api.libs.json.JsValue
import play.api.libs.json.JsString
import play.api.libs.json.JsArray
import play.api.libs.json.JsObject
import play.api.libs.json.JsResult
import play.api.libs.json.Reads._
import play.api.libs.json.Json.JsValueWrapper

// spray Json
import spray.json._
import DefaultJsonProtocol._ 

// smart Json
import java.util
import net.minidev.json.{JSONObject}
import net.minidev.json.parser.JSONParser
import scala.collection.JavaConversions.mapAsScalaMap
import scala.collection.JavaConversions.mutableMapAsJavaMap

object JsonUtil {

    /**
     * play 解析类库
     */
    implicit val objectMapFormat = new Format[Map[String, Object]] {

      /**
       * 写, Map -> Json 操作
       */
      def writes(map: Map[String, Object]): JsValue = 
        Json.obj(map.map{case (s, o) =>
          val ret:(String, JsValueWrapper) = o match {
            case _:String => s -> JsString(o.asInstanceOf[String])
            case z:Map[String, String] => {
                s -> o.asInstanceOf[Map[String, String]]
            }
            case _ => s -> JsArray(o.asInstanceOf[List[String]].map(JsString(_)))
          }
          ret
        }.toSeq:_*)

      /**
       * 读，Json -> Map
       */
      def reads(jv: JsValue): JsResult[Map[String, Object]] =
        JsSuccess(jv.as[Map[String, JsValue]].map{case (k, v) =>
          k -> (v match {
            case s: JsString => s.as[String]
            case z: JsObject => {
                var rs: Map[String, String] = Map[String, String]()
                val jsonValue: JsValue = Json.parse(z.toString())
                val mp = Json.fromJson[Map[String, String]](jsonValue)
                if (mp != null) {
                    rs = mp.get
                }
                rs
            }
            case l => l.as[List[String]]
          })
        })
    }

    /**
     * play 类库 map -> json  
     * 
     * 使用前需要把 map 转换为不可变 map.toMap
     *  如果内部有嵌套 map 也需要转换为不可变 map.
     *    val map = Map("a"-> Map("a"->"2").toMap).toMap
     *      OR
     *    val map = mapData.map(f => f._1 -> f._2.toMap).toMap
     */
    def playMapToJson(map: Map[String, Object]) : String =  {
        val jv: JsValue = Json.toJson(map)
        jv.toString()
    }

    /**
     * json -> map 不可变 Map :
     * 
     *  // 案例
     *  val userNeedsBaseData = JsonUtil.playJsonToMap(userNeedsJson) // 返回的是一个 Map[String, Object]
        val userNeedsBaseDataFormat = userNeedsBaseData.map{case (k,v) =>
            val curK = k 
            // 把元祖 v 转换为 Map, 再把 map 转换为可变 Map
            val curV = scala.collection.mutable.Map(v.asInstanceOf[scala.collection.immutable.Map[String,String]].toSeq:_*)
            k -> curV
        }
        // 再把最外层的 Map 也转换为可变的 map
        var userNeedsBase = collection.mutable.Map(userNeedsBaseDataFormat.toSeq:_*).asInstanceOf[scala.collection.mutable.Map[String, Map[String, String]]]
     */
    def playJsonToMap(jsonStr: String): Map[String, Object] = {
        val jsonValue: JsValue = Json.parse(jsonStr)
        val jr: JsResult[Map[String, Object]] = Json.fromJson[Map[String, Object]](jsonValue)
        jr.get
    }

    /**
     * Json -> JsValue 解析成 JsValue 对象
     * json.\("fieldName") 使用这种方式直接访问
     * 或者直接 .toString 即可访问完整的
     */
    def playJsonToJsValue(jsonString: String) : JsValue = {
        val jsonValue: JsValue = Json.parse(jsonString)
        jsonValue
    }


    
    def playTest() : Unit = {
        // map 转换成为 String
        val map: Map[String, Object] = Map(
                "val1" -> "xxx",
                "val2" -> List("a", "b", "c"), 
                "val3" -> "sss",
                "val4" -> List("d", "e", "f"),
                "val5" -> Map("a"->"1", "b"->"2", "c"->"3").toMap // 你懂得,转换为不可变 Map
        )
        val jv: JsValue = Json.toJson(map)
        println(jv) // {"val1":"xxx","val3":"sss","val2":["a","b","c"],"val5":{"a":"1","b":"2","c":"3"},"val4":["d","e","f"]}

        // String 转换为 Map
        val jr: JsResult[Map[String, Object]] = Json.fromJson[Map[String, Object]](jv)
        println(jr.get) //Map(val1 -> xxx, val3 -> sss, val2 -> List(a, b, c), val5 -> Map(a -> 1, b -> 2, c -> 3), val4 -> List(d, e, f))
        println(jr.get("val5").asInstanceOf[Map[String, String]].get("a"))
        
        
        val uesrTagData: Map[String, Map[String,String]] = Map[String, Map[String,String]](
              "0" ->   Map(
                "city" -> "1",
                "block" -> "1",
                "community" -> "1",
                "bedrooms" -> "2"
                ),
               "1" ->   Map(
                "city" -> "1",
                "block" -> "1",
                "community" -> "1",
                "bedrooms" -> "2"
                )
        )
        val uesrTagToJson: JsValue = Json.toJson(uesrTagData)
        println(uesrTagToJson)
        val uesrTagToMap: JsResult[Map[String, Object]] = Json.fromJson[Map[String, Object]](uesrTagToJson)
        println(uesrTagToMap.getOrElse().asInstanceOf[Map[String, Map[String,String]]])
        //exit
    }



    /**
     *  scala 原生对象 json -> object
     */
    def JsonToObj(jsonString: String) : Option[Any] = {
        val obj =  JSON.parseFull(jsonString)
        obj
    }


    import scala.collection.mutable.Map
    /**
     * 将map转为json
     * @param map 输入格式 mutable.Map[String,Object]
     * @return
     * */
    def smartMapToJsonStr(map : Map[String,Object]) : String = {
        val jsonString = JSONObject.toJSONString(map)
        jsonString
    }

   /**
   * 将 json 转化为 Map
   * @param json 输入json字符串
   * @return
   * */
    def smartJsonStrToMap(json : String) : Map[String,Object] = {
        val map : Map[String,Object]= Map()
        val jsonParser =new JSONParser()

        //将string转化为jsonObject
        val jsonObj: JSONObject = jsonParser.parse(json).asInstanceOf[JSONObject]
    
        //获取所有键
        val jsonKey = jsonObj.keySet()
    
        val iter = jsonKey.iterator()
    
        while (iter.hasNext){
          val field = iter.next()
          val value = jsonObj.get(field).toString
    
          if(value.startsWith("{")&&value.endsWith("}")){
            val value = mapAsScalaMap(jsonObj.get(field).asInstanceOf[util.HashMap[String, String]])
            map.put(field,value)
          }else{
            map.put(field,value)
          }
        }
        map
    }

}