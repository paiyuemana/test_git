package udf.myCodeUdf;

import java.io.IOException;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
//import org.apache.commons.logging.LogFactory;

import com.paiyue.audience.full.AudienceDatabase;


/**
 * Hello world!
 *
 */
public class App extends EvalFunc<String>{
	private static  final AudienceDatabase ad = new AudienceDatabase();
	@Override
	public String exec(Tuple input) throws IOException {
		if (input == null || input.size() == 0) {
		      return null;
		}
		Log log =this.getLogger();
		String key="";
		try{
			key=(String)input.get(0);
			log.debug("UDF.con : "+key);
		}catch(Exception e){
			log.debug("UDF.fun : "+e.getMessage());
		}
		if(key==null){
			return "";
		}else{
			return key+key;
		}
//		try{
//			if(input.get(0)!=null){
//				String key=(String)input.get(0);
//				Map.Entry<Long, Map<Short, Double>> result = ad.getAudience(key);
//				if(result!=null){
//					return key;
//				}else{
//					return null;
//				}
//			}
//		}catch(IOException e){
//			throw new IOException("error about audience database!");
//		}
//		return null;
	}
	public String transMapToString(Map<Short, Double> map) {
		StringBuffer sb = new StringBuffer(200);
		for (Map.Entry<Short, Double> entry : map.entrySet()) {
			sb.append(entry.getKey()).append('=')
					.append(entry.getValue().toString()).append(';');
//			sb.append(entry.getKey())
//			.append(entry.getValue().toString());
		}
		return sb.toString();
	}	
}



