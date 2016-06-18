package org.locationtech.geomesa.api;

import com.vividsolutions.jts.geom.Geometry;
import org.geotools.filter.identity.FeatureIdImpl;
import org.locationtech.geomesa.utils.geotools.SftBuilder;
import org.locationtech.geomesa.utils.stats.Cardinality;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import java.util.Date;

public class DefaultSimpleFeatureView<T> implements SimpleFeatureView<T> {

    private String name;

    private SimpleFeatureType sft;

    public DefaultSimpleFeatureView(String name) {
        this.name = name;
        this.sft = new SftBuilder()
                .date("dtg", true, true)
                .bytes("payload", new SftBuilder.Opts(false, false, false, Cardinality.UNKNOWN()))
                .geometry("geom", true)
                .userData("geomesa.mixed.geometries", "true")
                .build(name);
    }

    @Override
    public void populate(SimpleFeature f, T t, String id, byte[] payload, Geometry geom, Date dtg) {
        f.setAttribute("geom", geom);
        f.setAttribute("dtg", dtg);
        f.setAttribute("payload", payload);
        ((FeatureIdImpl) f.getIdentifier()).setID(id);
    }

    @Override
    public SimpleFeatureType getSimpleFeatureType() {
        return sft;
    }

}
