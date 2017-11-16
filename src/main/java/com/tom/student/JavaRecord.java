package com.tom.student;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class JavaRecord implements java.io.Serializable {
  private String ip;
  private String identity;
  private String userid;
  private String requestTime;
  private String requestUri;
  private String status;
  private String count;
  private String referrer;
  private String userAgent;
  private static Pattern pat = null;


  public void init(String line) {
      if (pat==null) {
          //String rgx = "^([0-9.]+) ([\\w.-]+) ([\\w.-]+) ([[^[]]+]) \"((?:[^\"]|\")+)\" (\\d{3}) (\\d+|[-]) \"((?:[^\"]|\")+)\" \"((?:[^\"]|\")+)\"$";
          String rgx = "^([0-9.]+) ([\\w.-]+) ([\\w.-]+) (\\[[^\\]]+\\]) \"([^\"]+)\" (\\d{3}) (\\d+|[-]) \"([^\"]+)\" \"([^\"]+)\"$";
          pat = Pattern.compile(rgx);
      }
      Matcher m = pat.matcher(line);
      if (m.find()) {
          ip = m.group(1);
          identity = m.group(2);
          userid = m.group(3);
          requestTime = m.group(4);
          requestUri = m.group(5);
          status = m.group(6);
          count = m.group(7);
          referrer = m.group(8);
          userAgent = m.group(9);
      } else {
          ip = "";
          identity = "";
          userid = "";
          requestTime = "";
          requestUri = "";
          status = "";
          count = "";
          referrer = "";
          userAgent = "";
      }
  }

  public String getIp() {
    return ip;
  }
  public String getIdentity() {
    return identity;
  }
  public String getUserid() {
    return userid;
  }
  public String getRequestTime() {
    return requestTime;
  }
  public String getRequestUri() {
    return requestUri;
  }
  public String getStatus() {
    return status;
  }
  public String getCount() {
    return count;
  }
  public String getReferrer() {
    return referrer;
  }
  public String getUserAgent() {
    return userAgent;
  }

  public void setIp(String ip) {
    this.ip = ip;
  }
  public void setIdentity(String identity) {
    this.identity = identity;
  }
  public void setUserid(String userid) {
    this.ip = userid;
  }
  public void setRequestTime(String requestTime) {
    this.requestTime = requestTime;
  }
  public void setRequestUri(String requestUri) {
    this.requestUri = requestUri;
  }
  public void setStatus(String status) {
    this.status = status;
  }
  public void setCount(String count) {
    this.count = count;
  }
  public void setReferrer(String referrer) {
    this.referrer = referrer;
  }
  public void setUserAgent(String userAgent) {
    this.userAgent = userAgent;
  }
}
