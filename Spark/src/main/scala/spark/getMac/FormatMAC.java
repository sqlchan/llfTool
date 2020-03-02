package spark.getMac;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class FormatMAC {
    private static final Log LOG = LogFactory.getLog(FormatMAC.class);

    public TerminalInfo handle(String msg){
        TerminalInfo tmpTerminalInfo = new TerminalInfo();
        JSONObject jsonObject = JSONObject.parseObject(msg);
        JSONObject ll = jsonObject.getJSONArray("macDevice").getJSONObject(0);
        String mac = ll.get("macAddress").toString();
        if (null == mac || mac.length() != 12) {
            return null;
        }
        String devNo = ll.get("deviceNo").toString();
        if (devNo.isEmpty()) {
            return null;
        }
        long collectTime = Long.parseLong((!ll.get("collectTime").toString().isEmpty()) ? ll.get("collectTime").toString() : "0");
        tmpTerminalInfo.setMACAddress(mac);
        tmpTerminalInfo.setCollectTime(collectTime);
        tmpTerminalInfo.setMsgType( ll.getIntValue("msgType"));
        tmpTerminalInfo.setFirstCollectTime(collectTime);
        tmpTerminalInfo.setLastCollectTime(collectTime);
        if(ll.containsKey("scanTime"))tmpTerminalInfo.setScanTime( ll.getIntValue("scanTime"));
        if(ll.containsKey("siteNo"))tmpTerminalInfo.setSiteNo( ll.getString("siteNo"));
        if(ll.containsKey("fieldIntensity"))tmpTerminalInfo.setFieldIntensity(Double.parseDouble((!ll.get("fieldIntensity").toString().isEmpty()) ? ll.get("fieldIntensity").toString() : "0"));
        if(ll.containsKey("historySSID"))tmpTerminalInfo.setHistorySSID( ll.get("historySSID").toString());
        if(ll.containsKey("inOutFlag"))tmpTerminalInfo.setInOutFlag( ll.getIntValue("inOutFlag"));
        if(ll.containsKey("linkageDevCode"))tmpTerminalInfo.setLinkageDevCode((String) ll.get("linkageDevCode"));
        tmpTerminalInfo.setDevNo(devNo);
        if(ll.containsKey("deviceType"))tmpTerminalInfo.setDevType(ll.getIntValue("deviceType"));
        JSONObject targetAttrs = null;
        if(ll.containsKey("targetAttrs"))targetAttrs = ll.getJSONObject("targetAttrs");
        if(targetAttrs != null){
            tmpTerminalInfo.setAreaCode(targetAttrs.containsKey("regionIndexCode")?targetAttrs.get("regionIndexCode").toString() : "");
            LOG.info(targetAttrs.get("regionIndexCode").toString());
        }
        JSONObject coordinate = null;
        if(ll.containsKey("coordinate"))coordinate = ll.getJSONObject("coordinate");
        if(coordinate != null){
            if(coordinate.containsKey("x"))tmpTerminalInfo.setX(Double.valueOf((!coordinate.get("x").toString().isEmpty()) ? coordinate.get("x").toString() : "0"));
            if(coordinate.containsKey("y"))tmpTerminalInfo.setY(Double.valueOf((!coordinate.get("y").toString().isEmpty()) ? coordinate.get("y").toString() : "0"));
        }
        if(ll.containsKey("aPMacAddress"))tmpTerminalInfo.setAPMacAddress(ll.get("aPMacAddress").toString());
        if(ll.containsKey("terminalBrand"))tmpTerminalInfo.setTerminalBrand( ll.get("terminalBrand").toString());
        if(ll.containsKey("longitude"))tmpTerminalInfo.setCollectDevLongitude(Double.parseDouble((!ll.get("longitude").toString().isEmpty()) ? ll.get("longitude").toString() : "0"));
        if(ll.containsKey("latitude"))tmpTerminalInfo.setCollectDevDimensionality(Double.parseDouble((!ll.get("latitude").toString().isEmpty()) ? ll.get("latitude").toString() : "0"));
        if(ll.containsKey("version"))tmpTerminalInfo.setVersion( ll.get("version").toString());
        if(ll.containsKey("vendor"))tmpTerminalInfo.setVendor( ll.get("vendor").toString());
        tmpTerminalInfo.setCollectTimeRecord(ll.get("collectTime").toString());
        tmpTerminalInfo.setLinkBodyMacId(ll.containsKey("linkBodyMacId") ? ll.get("linkBodyMacId").toString() : "");
        tmpTerminalInfo.setLinkFaceMacId(ll.containsKey("linkFaceMacId") ? ll.get("linkFaceMacId").toString() : "");
        tmpTerminalInfo.setLinkRFIDMacId(ll.containsKey("linkRfidMacId") ? ll.get("linkRfidMacId").toString() : "");
        tmpTerminalInfo.setLinkVehicleMacId(ll.containsKey("linkVehicleMacId") ? ll.get("linkVehicleMacId").toString() : "");
        //tmpTerminalInfo.setAreaCode(ll.containsKey("areaCode")?ll.get("areaCode").toString() : "");
        return tmpTerminalInfo;
    }
}
