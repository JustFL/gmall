package bean

//CREATE TABLE user_info(id varchar PRIMARY KEY, info.gender varchar, info.birthday varchar, age_group varchar, gender_name varchar) salt_buckets = 3;
case class UserInfo (id: String, gender: String, birthday: String, var age_group:String, var gender_name:String)
