import com.qlangtech.tis.plugin.iceberg.datax.TestDataxIcebergWriter;
import com.qlangtech.tis.plugin.iceberg.datax.TestIcebergDescriptors;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * 测试套件
 * <p>
 * 父 pom 的 surefire 只 include {@code **}{@code /TestAll.java}，所有单测都要在这里登记
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({TestDataxIcebergWriter.class, TestIcebergDescriptors.class})
public class TestAll {
}
