

import org.scalatra._
import org.scalatra.LifeCycle
import javax.servlet.ServletContext
import com.yogini.twitter.analyzer.Servlet

class ScalatraBootstrap extends LifeCycle {

  override def init(context: ServletContext) {
    context.mount(new Servlet, "/*")
  }
}