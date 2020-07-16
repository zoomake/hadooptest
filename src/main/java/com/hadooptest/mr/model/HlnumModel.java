package com.hadooptest.mr.model;

import com.hadooptest.utils.Constant;

public class HlnumModel {

    //判断该记录格式是否正确
    private Boolean is_validate = true;
    //需要解析的属性为8个
    private String remote_user;//用户uid
    private String request_time;//访问时间
    private String request_method;//请求方法
    private String request_status;//请求方法

    public static HlnumModel parse(String line) {
        String[] arr = line.split(" ");
        HlnumModel hlnum = new HlnumModel();
        if(arr.length < 10){
            hlnum.setIs_validate(false);
        } else {
            hlnum.setRequest_time(arr[0] + " " +  arr[1].split("\\.")[0]);
            hlnum.setRequest_method(arr[9]);
//            hlnum.setRequest_method(arr[9].split(":")[1]);
            hlnum.setRequest_status(arr[2].replace("|-", ""));
            if(!Constant.REQUEST_STATUS_INFO.equals(hlnum.getRequest_status()) || null == hlnum.getRequest_method()) {
                hlnum.setIs_validate(false);
            }
        }
        return hlnum;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("valid:").append(this.is_validate);
        sb.append("\nremote_user:").append(this.remote_user);
        sb.append("\nrequest_time:").append(this.request_time);
        sb.append("\nrequest_method:").append(this.request_method);
        return sb.toString();
    }

    public static void main(String[] args) {
        //数据格式:
        String line = "2020-06-12 19:03:18.820 |-INFO  [http-nio-8581-exec-10] com.chinaso.contribution.filter.UrlFileter [45] -| request url:/articles/getCity";
        HlnumModel hlnum = HlnumModel.parse(line);
        System.out.println(hlnum.toString());
    }

    public String getRequest_status() {
        return request_status;
    }

    public void setRequest_status(String request_status) {
        this.request_status = request_status;
    }

    public String getRemote_user() {
        return remote_user;
    }

    public void setRemote_user(String remote_user) {
        this.remote_user = remote_user;
    }

    public String getRequest_time() {
        return request_time;
    }

    public void setRequest_time(String request_time) {
        this.request_time = request_time;
    }

    public Boolean getIs_validate() {
        return is_validate;
    }

    public void setIs_validate(Boolean is_validate) {
        this.is_validate = is_validate;
    }

    public String getRequest_method() {
        return request_method;
    }

    public void setRequest_method(String request_method) {
        this.request_method = request_method;
    }
}
