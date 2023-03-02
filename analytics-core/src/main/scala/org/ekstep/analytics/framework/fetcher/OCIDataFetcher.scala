

import org.ekstep.analytics.framework.Query
import scala.collection.mutable.Buffer
import org.ekstep.analytics.framework.util.CommonUtil
import scala.collection.mutable.ListBuffer
import org.ekstep.analytics.framework.exception.DataFetcherException
import org.ekstep.analytics.framework.exception.DataFetcherException
import org.joda.time.LocalDate
import java.util.Date
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework.FrameworkContext

/**
 * @author Santhosh
 */
object OCIDataFetcher {

    @throws(classOf[DataFetcherException])
    def getObjectKeys(queries: Array[Query])(implicit fc: FrameworkContext): Array[String] = {

        val keys = for(query <- queries) yield {
            val paths = if(query.folder.isDefined && query.endDate.isDefined && query.folder.getOrElse("false").equals("true")) {
                Array("https://objectstorage." + getRegion(query.region)+".oraclecloud.com/n/" + getCompartment(query.compartment) 
                + "/b/"+getBucket(query.bucket)+"/o/" + getObjectName(query.objectName) + query.endDate.getOrElse(""))
            } else {
                getKeys(query);
            }
            if(query.excludePrefix.isDefined) {
                paths.filter { x => !x.contains(query.excludePrefix.get) }
            } else {
                paths
            }
        }
        
        keys.flatMap { x => x.map { x => x } }
    }
    
    private def getKeys(query: Query)(implicit fc: FrameworkContext) : Array[String] = {
        val storageService = fc.getStorageService("oci");
        val keys = storageService.searchObjects(getRegion(query.region), getCompartment(query.compartment), getBucket(query.bucket), getObjectName(query.objectName),
        query.startDate, query.endDate, query.delta, query.datePattern.getOrElse("yyyy-MM-dd"))
        storageService.getPaths(getBucket(query.bucket), keys).toArray
    }
    
    private def getBucket(bucket: Option[String]) : String = {
        bucket.getOrElse("b/");
    }

    private def getCompartment(compartment: Option[String]) : String = {
        compartment.getOrElse("n/");
    }

    private def getRegion(region: Option[String]) : String = {
        region.getOrElse("objectstorage");
    }

    private def getObjectName(objectName: Option[String]) : String = {
        objectName.getOrElse("o/");
    }

}