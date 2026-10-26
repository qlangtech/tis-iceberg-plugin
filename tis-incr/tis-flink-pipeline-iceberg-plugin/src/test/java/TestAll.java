import com.qlangtech.tis.plugins.incr.flink.pipeline.iceberg.sink.TestIcebergPipelineSinkFactory;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * 增量模块的单测入口。
 * <p>
 * surefire 默认只认 {@code *Test.java}，本模块的测试类以 {@code Test} 开头（与 TIS 其他插件一致），
 * 因此通过 {@code TestAll} 套件挂进去。新增测试类时记得在这里登记，否则不会被执行。
 *
 * @author 百岁（baisui@qlangtech.com）
 * @date 2026/10/26
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({TestIcebergPipelineSinkFactory.class})
public class TestAll {
}
