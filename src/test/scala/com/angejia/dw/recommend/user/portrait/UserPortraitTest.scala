package com.angejia.dw.recommend.user.portrait

import collection.mutable.Stack
import org.scalatest._

class UserPortraitTest extends FlatSpec with Matchers {

    "new log" should "be parsed" in {
        val log = ("0.045\t0.045\t183.198.218.230\t1378\t127.0.0.1:9000\t"
            + "[2016-11-23T16:59:15+08:00]\tapi.angejia.com\t"
            + "GET /mobile/member/inventories/1/2 HTTP/1.1\t"
            + "200\t379\t-\tDalvik/2.1.0 (Linux; U; Android 6.0.1; OPPO R9s Build/MMB29M)\t"
            + "0.46\t183.198.218.230\t"
            + "ExhuBkt16RSHa0/0C+y9x4sAxoD7EKhTTIoMs76vHmF4082Rl0cMQrHG/n4sx5Swb2xKFoKuT0q71Fh1+vcW/wo7KxexQroTAfJbSze3I5pDxw6TdZ/8HoE2wwmq0Zfcoevuqh00RFkCTG88w2CddEjoOiL6xJ1PI0GK4i4D5MYk3WvuFyoYcBZ+Nk5i4yVhrD8GCRVu8uRiXCoQAyX8mahVxbtae3MEyOZD4E2goMoiul9tEcXyK0XYB+aJhEZVL6jdCR5kg9bihbVyulmWOIvjLnqkZCjBcEFrpv9kcG9zNTY9MUTUyJxPg2MuvzeZZ2FG5Yv8GKIhOSSNl1pKY/v35cpXBzldw55381DmC1s=\t"
            + "app=a-angejia;av=4.8.1;ccid=2;gcid=;ch=B14;lng=;lat=;net=WIFI;p=android;pm=Android-OPPO R9s;osv=6.0.1;dvid=86253103615747802:00:00:00:00:00;uid=760198\t"
            + "-\t-")
        val res = UserPortrait.formatLogData(log)

        res should contain key ("logRequestUri")
        res.get("logRequestUri") should equal(Some("/mobile/member/inventories/1/2"))

        res should contain key ("logHost")
        res.get("logHost") should equal(Some("api.angejia.com"))

        res should contain key ("logTime")
        res.get("logTime") should equal(Some("[2016-11-23T16:59:15+08:00]"))

        res should contain key ("userId")
        //res.get("userId") should equal(Some("728924"))

        res should contain key ("appAgent")
        res.get("appAgent") should equal(Some("app=a-angejia;av=4.8.1;ccid=2;gcid=;ch=B14;lng=;lat=;net=WIFI;p=android;pm=Android-OPPO R9s;osv=6.0.1;dvid=86253103615747802:00:00:00:00:00;uid=760198"))

        res should contain key ("cityId")
        res.get("cityId") should equal(Some("2"))

        res should contain key ("auth")
        res.get("auth") should equal(Some("ExhuBkt16RSHa0/0C+y9x4sAxoD7EKhTTIoMs76vHmF4082Rl0cMQrHG/n4sx5Swb2xKFoKuT0q71Fh1+vcW/wo7KxexQroTAfJbSze3I5pDxw6TdZ/8HoE2wwmq0Zfcoevuqh00RFkCTG88w2CddEjoOiL6xJ1PI0GK4i4D5MYk3WvuFyoYcBZ+Nk5i4yVhrD8GCRVu8uRiXCoQAyX8mahVxbtae3MEyOZD4E2goMoiul9tEcXyK0XYB+aJhEZVL6jdCR5kg9bihbVyulmWOIvjLnqkZCjBcEFrpv9kcG9zNTY9MUTUyJxPg2MuvzeZZ2FG5Yv8GKIhOSSNl1pKY/v35cpXBzldw55381DmC1s="))

        res should contain key ("logType")
        res.get("logType") should equal(Some("accessLog"))
    }

    "old log" should "be parsed" in {
        val log =("0.064\t0.064\t153.99.123.51\t1529\t127.0.0.1:9000\t"
                +"[2016-11-01T00:00:00+08:00]\tapi.angejia.com\t"
                +"GET /mobile/member/inventories/1/2 HTTP/1.1\t"
                +"200\t2195\t-\tAngejia/4.6.2 CFNetwork/808.0.2 Darwin/16.0.0\t7.42\t153.99.123.51\t"
                +"kCp+SLcl85sKrn/1jntFnhRXZlG79zMr6wEAy7Vkd9TyJ46da3IxyJPRLdd/ngMk/KqLmF8p26/izeoN7/Pgo7NB5VO21FyaHKrN370snfqWOv5CYb1x7fFJNJQYwwX54ketZAJ1mMSWj7LzbhSj9Kedl56dUi/9OL64djEld2iecKGWtNk2Rc4I2FWjoLiavAsJh/6RCOJ84tcc7KLB+IeCjz/uW3JlrZoJO3qvDfMiCv28y6geQjRNVljmBo3P\t"
                +"app=i-angejia;av=4.6;ccid=1;gcid=1;ch=A01;lng=0.000000;lat=0.000000;ip=192.168.1.100;mac=None;net=WIFI;p=iOS;pm=iPhone9,1;osv=10.0.1;dvid=09DF78A6-935D-46E6-9BB5-201610241142;uid=728924;idfa=D2CAED51-9235-4B51-9CC6-7ECC3AE7DD91\t"
                +"-")
        val res = UserPortrait.formatLogData(log)

        res should contain key ("logRequestUri")
        res.get("logRequestUri") should equal(Some("/mobile/member/inventories/1/2"))

        res should contain key ("logHost")
        res.get("logHost") should equal(Some("api.angejia.com"))

        res should contain key ("logTime")
        res.get("logTime") should equal(Some("[2016-11-01T00:00:00+08:00]"))

        res should contain key ("userId")
        res.get("userId") should equal(Some("728924"))

        res should contain key ("appAgent")
        res.get("appAgent") should equal(Some("app=i-angejia;av=4.6;ccid=1;gcid=1;ch=A01;lng=0.000000;lat=0.000000;ip=192.168.1.100;mac=None;net=WIFI;p=iOS;pm=iPhone9,1;osv=10.0.1;dvid=09DF78A6-935D-46E6-9BB5-201610241142;uid=728924;idfa=D2CAED51-9235-4B51-9CC6-7ECC3AE7DD91"))

        res should contain key ("cityId")
        res.get("cityId") should equal(Some("1"))

        res should contain key ("auth")
        res.get("auth") should equal(Some("kCp+SLcl85sKrn/1jntFnhRXZlG79zMr6wEAy7Vkd9TyJ46da3IxyJPRLdd/ngMk/KqLmF8p26/izeoN7/Pgo7NB5VO21FyaHKrN370snfqWOv5CYb1x7fFJNJQYwwX54ketZAJ1mMSWj7LzbhSj9Kedl56dUi/9OL64djEld2iecKGWtNk2Rc4I2FWjoLiavAsJh/6RCOJ84tcc7KLB+IeCjz/uW3JlrZoJO3qvDfMiCv28y6geQjRNVljmBo3P"))

        res should contain key ("logType")
        res.get("logType") should equal(Some("accessLog"))
    }
}
