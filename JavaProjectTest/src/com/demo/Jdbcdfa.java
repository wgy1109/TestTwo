package com.demo;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

// 一个修改后的中文敏感词校验方法，
// DFA算法是初始化后把数据整合为 {白={粉={isEnd=1}, isEnd=0}, 大={麻={isEnd=1}, isEnd=0, 坏={蛋={isEnd=1}, isEnd=0}}}样式的算法。
// 做中文敏感词计算甚佳
public class Jdbcdfa {
	
	public final static String driverClassName = "org.postgresql.Driver";
	public final static String url = "jdbc:postgresql://localhost:8080/test_yang";
	public final static String userName = "postgre";
	public final static String passWord = "postgre";
	public static final int EXPORTEVERYSIZEOFDB = 1000;
	
	public Map sensitiveWordMap = null;
	public static int minMatchTYpe = 1;      //最小匹配规则
	public static int maxMatchType = 2; 

	public static void main(String[] args) {
		String sql = "select content from sms_sensitive_word ";
		
		Jdbcdfa filter = new Jdbcdfa();  
		filter.JDBCexcelFile(sql, "content");
        System.out.println("敏感词的数量：" + filter.sensitiveWordMap.size());  
        String string = "太多的伤感情怀也许只局限于饲养基地 荧幕中的情节，主人公尝试着去用某种方式渐渐的很潇洒地释自杀指南怀那些自己经历的伤感。"  
                        + "然后法.轮.功 我们的扮演的角色就是跟随着主人公的喜红客联盟 怒哀乐而过于牵强的把自己的情感也附加于银幕情节中，然后感动就流泪，"  
                        + "难过就躺在某一个人的怀里尽情的阐述心扉或者手机卡复制器一个人一杯红酒一部电影在夜三.级.片 深人静的晚上，关上电话静静的发呆着。";  
        System.out.println("待检测语句字数：" + string.length());  
        long beginTime = System.currentTimeMillis();  
        Set<String> set = filter.getSensitiveWord(string, 1);  
        long endTime = System.currentTimeMillis();  
        System.out.println("语句中包含敏感词的个数为：" + set.size() + "。包含：" + set);  
        System.out.println("总共消耗时间为：" + (endTime - beginTime));

	}
	

	public void JDBCexcelFile(String sql,  String key ) {
//	    logger.info("数据库查询sql：" + sql + ",时刻：" + DateHelper.getDateString(null, "yyyy-MM-dd HH:mm:ss.SSS"));
		Connection conn = null;
        ResultSet rs = null;
        PreparedStatement ps =  null;
        List<Map<String, Object>> smsList = new ArrayList<Map<String, Object>>();
        Set<String> setlist = new HashSet<String>();
//        ExcelUtil excelutil = new ExcelUtil();
//		DecimalFormat df = new DecimalFormat("###############0.##");
        
        try {

            Class.forName("org.postgresql.Driver");

	    } catch (ClassNotFoundException e) {
	
	            System.out.println("Where is your PostgreSQL JDBC Driver? "
	                            + "Include in your library path!");
	            e.printStackTrace();
	            return;
	
	    }
        
        try {
//            Class.forName("org.postgresql.Driver").newInstance();
            conn = DriverManager.getConnection(url, userName, passWord);
            conn.setAutoCommit(false); //并不是所有数据库都适用，比如hive就不支持，orcle不需要
            ps = conn.prepareStatement(sql);
            ps.setFetchSize(EXPORTEVERYSIZEOFDB); 
            rs = ps.executeQuery();
            int i = 0;
//            logger.info("读取数据：" + i + ",时刻：" + DateHelper.getDateString(null, "yyyy-MM-dd HH:mm:ss.SSS"));
            while (rs.next()) {
            	setlist.add(rs.getString(key));
            }
            
            addSensitiveWordToHashMap(setlist);
            
        } catch (Throwable e) {
            e.printStackTrace();
        }finally{
        	if (rs != null) {
				try {
					rs.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
			if (ps != null) {
				try {
					ps.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
			if (conn != null) {
				try {
					conn.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
        }	
	}
	
	
	
	private void addSensitiveWordToHashMap(Set<String> keyWordSet) {  
	        sensitiveWordMap = new HashMap(keyWordSet.size());     //初始化敏感词容器，减少扩容操作  
	        String key = null;    
	        Map nowMap = null;  
	        Map<String, String> newWorMap = null;  
	        //迭代keyWordSet  
	        Iterator<String> iterator = keyWordSet.iterator();  
	        while(iterator.hasNext()){  
	            key = iterator.next();    //关键字  
	            nowMap = sensitiveWordMap;  
	            for(int i = 0 ; i < key.length() ; i++){  
	                char keyChar = key.charAt(i);       //转换成char型  
	                Object wordMap = nowMap.get(keyChar);       //获取  
	                if(wordMap != null){        //如果存在该key，直接赋值  
	                    nowMap = (Map) wordMap;  
	                }  
	                else{     //不存在则，则构建一个map，同时将isEnd设置为0，因为他不是最后一个  
	                    newWorMap = new HashMap<String,String>();  
	                    newWorMap.put("isEnd", "0");     //不是最后一个  
	                    nowMap.put(keyChar, newWorMap);  
	                    nowMap = newWorMap;  
	                }  
	                if(i == key.length() - 1){  
	                    nowMap.put("isEnd", "1");    //最后一个  
	                }  
	            }  
	        }  
	    }  
	 
	 public int CheckSensitiveWord(String txt,int beginIndex,int matchType){
	        boolean  flag = false;    //敏感词结束标识位：用于敏感词只有1位的情况  
	        int matchFlag = 0;     //匹配标识数默认为0  
	        char word = 0;  
	        Map nowMap = sensitiveWordMap;  
	        for(int i = beginIndex; i < txt.length() ; i++){  
	            word = txt.charAt(i);  
	            nowMap = (Map) nowMap.get(word);     //获取指定key  
	            if(nowMap != null){     //存在，则判断是否为最后一个  
	                matchFlag++;     //找到相应key，匹配标识+1   
	                if("1".equals(nowMap.get("isEnd"))){       //如果为最后一个匹配规则,结束循环，返回匹配标识数  
	                    flag = true;       //结束标志位为true     
//	                    if(SensitivewordFilter.minMatchTYpe == matchType){    //最小规则，直接返回,最大规则还需继续查找  
//	                        break;  
//	                    }  
	                }  
	            }  
	            else{     //不存在，直接返回  
	                break;  
	            }  
	        }  
	        if(matchFlag < 2 && !flag){       
	            matchFlag = 0;  
	        }  
	        return matchFlag;  
	    }  
	 
		public Set<String> getSensitiveWord(String txt , int matchType){
			Set<String> sensitiveWordList = new HashSet<String>();
			for(int i = 0 ; i < txt.length() ; i++){
				int length = CheckSensitiveWord(txt, i, matchType);    //判断是否包含敏感字符
				if(length > 0){    //存在,加入list中
					sensitiveWordList.add(txt.substring(i, i+length));
					i = i + length - 1;    //减1的原因，是因为for会自增
				}
			}
			return sensitiveWordList;
		}
	 
	 

}
