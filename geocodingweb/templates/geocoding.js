$( document ).ready(function() {
    // Initialize that Map.
    var map = L.map('map').setView([0, 0], 2);

    L.tileLayer('http://maps.tableausoftware.com:80/tile/d/mode=named%7Cfrom=tableau4_2_base/mode=named%7Cfrom=tableau4_2_water/mode=named%7Cfrom=tableau4_2_landcover/mode=named%7Cfrom=tableau4_2_water/mode=named%7Cfrom=tableau4_2_admin0_borders/mode=named%7Cfrom=tableau4_2_admin0_labels/mode=named%7Cfrom=tableau4_2_admin1_borders/mode=named%7Cfrom=tableau4_2_admin1_labels/ol/{z}/{x}/{y}@2x.png?apikey=tabmapbeta&size=304', {
        attribution: 'Map data &copy; <a href="http://openstreetmap.org">OpenStreetMap</a> contributors, <a href="http://creativecommons.org/licenses/by-sa/2.0/">CC-BY-SA</a>, Imagery © <a href="http://mapbox.com">Mapbox</a>',
        maxZoom: 16
    }).addTo(map);

    $( ".test-btn" ).click(function(event) {
        var test_type = event.target.id;
        var url = "";
        switch (test_type) {
            case "testinvalidshape":
                url = "http://localhost:7070/geocoding/testinvalidShape";
                break;
            case "testoverlappingpolygons":
                url = "http://localhost:7070/geocoding/testoverlappingpolygons";
                break;
            default:
                alert("Invalid Test Type");
        } 

        $.ajax({
            url: url,
            data: {},
            crossDomain:true,
            success: function (data) {
                console.log(data)
                //console.log(data['properties']['id'])
            },
            error: function () {
                // Remove the hide class, showing the bad input alert
                $('#form_entities_name_bad_request_alert').removeClass('hide');
            }
        });
    });
});

function populate_with_geojson_response(data) {
    if (data.features.length != 0) {
        // Update the map and table with geojson response from server.
        geoJsonFeatures.clearLayers();
        geoJsonFeatures.addData(data);
        // Re-center map around newly added data.
        // Padding required to not cut off markers at edges.
        map.fitBounds(geoJsonFeatures.getBounds().pad(0.5));
    }
}