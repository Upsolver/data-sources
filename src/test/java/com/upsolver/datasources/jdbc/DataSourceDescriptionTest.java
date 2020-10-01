package com.upsolver.datasources.jdbc;

import com.upsolver.common.datasources.DataSourceDescription;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.reflections.Reflections;

import java.util.Collection;

import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;

@RunWith(Parameterized.class)
public class DataSourceDescriptionTest {
    private final Class<? extends DataSourceDescription> clazz;

    public DataSourceDescriptionTest(Class<? extends DataSourceDescription> clazz) {
        this.clazz = clazz;
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection<?> dataSourceDescriptions() {
        return new Reflections("").getSubTypesOf(DataSourceDescription.class);
    }


    @Test
    public void test() throws ReflectiveOperationException {
        test(clazz);
    }


    private void test(Class<? extends DataSourceDescription> clazz) throws ReflectiveOperationException {
        DataSourceDescription obj = clazz.getDeclaredConstructor().newInstance();
        assertNotBlank(obj.getName());
        assertNotBlank(obj.getDescription());
        assertNotBlank(obj.getLongDescription());
        String icon = obj.getIconResourceName();
        assertNotBlank(icon);
        assertNotNull(getClass().getResource("/" + icon));
    }

    private void assertNotBlank(String str) {
        assertNotNull(str);
        assertNotEquals("", str);
    }
}
