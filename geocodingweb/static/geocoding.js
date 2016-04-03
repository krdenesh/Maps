$( document ).ready(function() {
    // Initialize that Map.
    var map = L.map('map').setView([0, 0], 2);

    L.tileLayer('http://maps.tableausoftware.com:80/tile/d/mode=named%7Cfrom=tableau4_2_base/mode=named%7Cfrom=tableau4_2_water/mode=named%7Cfrom=tableau4_2_landcover/mode=named%7Cfrom=tableau4_2_water/mode=named%7Cfrom=tableau4_2_admin0_borders/mode=named%7Cfrom=tableau4_2_admin0_labels/mode=named%7Cfrom=tableau4_2_admin1_borders/mode=named%7Cfrom=tableau4_2_admin1_labels/ol/{z}/{x}/{y}@2x.png?apikey=tabmapbeta&size=304', {
        attribution: 'Map data &copy; <a href="http://openstreetmap.org">OpenStreetMap</a> contributors, <a href="http://creativecommons.org/licenses/by-sa/2.0/">CC-BY-SA</a>, Imagery © <a href="http://mapbox.com">Mapbox</a>',
        maxZoom: 16
    }).addTo(map);

    var geoJsonFeatures = L.geoJson().addTo(map);
    geoJsonFeatures.options = {onEachFeature: function (feature, layer) {

        // The sorted order of our keys.
        var keys_sorted = ["id", "class", "map_code", "parent_id",
            "de_de", "en_us", "es_es", "fr_fr",
            "ja_jp", "ko_kr", "none", "pt_br", "zh_cn",
            "synonyms"];

        // Array to store html strings
        var html_output = [];

        // Iterate over our sorted list of keys, generating html for popup.
        keys_sorted.forEach(function (element, index, array) {
            if (feature.properties.hasOwnProperty(element)) {
                var html_string = "&lt;b&gt;" + element + ":&lt;/b&gt; " + feature.properties[element] + "&lt;br/&gt;";
                html_output.push(html_string);
            }
        });

        // Create the popup.
        layer.bindPopup(html_output.join(''));
    }};

    if($('input:radio[name=radio]:checked').val() == "csv"){
            $('#postgres-iput-details').hide();
            $('#csv-iput-details').show();
    }
    if($('input:radio[name=radio]:checked').val() == "postgres"){
            $('#csv-iput-details').hide();
            $('#postgres-iput-details').show();
    }
    $("input:radio[name=radio]").change(function(){
        if($('input:radio[name=radio]:checked').val() == "postgres"){
            $('#csv-iput-details').hide();
            $('#postgres-iput-details').show();
        }else{
            $('#postgres-iput-details').hide();
            $('#csv-iput-details').show();
        }
    });

    $( ".test-btn" ).click(function(event) {
        var input_data_type = $('input:radio[name=radio]:checked').val();
        var path_to_csv = '';
        var host = '';
        var database = '';
        var user = '';
        var password = '';
        var staging_prefix = '';
        var url_query = 'input_type=' + input_data_type;
        if(input_data_type == 'csv'){
            path_to_csv = $('#path-to-csv').val();
            url_query += "&path_to_csv=" + path_to_csv;
        }else{
            host = $('#host').val();
            url_query += "&host=" + host;
            database = $('#database').val();
            url_query += "&database=" + database;
            user = $('#user').val();
            url_query += "&user=" + user;
            password = $('#password').val();
            url_query += "&password=" + password;
            staging_prefix = $('#staging-prefix').val();
            url_query += "&staging_prefix=" + staging_prefix;
        }
        var btn = $(this);
        var test_type = event.target.id;
        var url = "";
        switch (test_type) {
            case "testinvalidshape":
                $('#test-result-' + test_type).empty();
                url = "http://localhost:7070/geocoding/testinvalidShape/?" + url_query;
                break;
            case "testoverlappingpolygons":
                $('#test-result-' + test_type).empty();
                url = "http://localhost:7070/geocoding/testoverlappingpolygons/?" + url_query;
                break;
            case "testpointinpolygon":
                $('#test-result-' + test_type).empty();
                url = "http://localhost:7070/geocoding/testpointinpolygon/?" + url_query;
                break;
            default:
                alert("Invalid Test Type");
        } 
        $(btn).css('background-color','#00FF00');
        $(btn).buttonLoader('start');
        test_results_html = "";
        $.ajax({
            url: url,
            data: {},
            success: function (data) {
                $(btn).buttonLoader('stop');
                var parsed_data = $.parseJSON(data);
                $('.test-result').bind('click', function (event) {
                    var target = $(event.target);
                   if (target.is('.test-result-link')) {
                      event.preventDefault();
                      var id = event.target.id;
                      console.log(data[id]);
                      var assetLayerGroup = new L.LayerGroup();
                      assetLayerGroup.clearLayers();
                      var group = L.geoJson(data[id]).addTo(map);
                      map.fitBounds(group.getBounds());
                   }
                });
                var items = [];
                $.each(parsed_data, function(index, val) {
                    items.push('<a class="test-result-link onmouseover="" style="cursor: pointer;" id="' + index + '">' + index + ',</a>');
                });

                var result_string = "<p><strong><font color='red'>Total number of failures for Test:"+test_type+"-> "+ items.length +"</font></strong>.</p>";
                $('#test-result-' + test_type).append(result_string);
                $('#test-result-' + test_type).append(items);
                //$(test_results_html).appendTo('#test-result-' + test_type);
            },
            error: function () {
                // Remove the hide class, showing the bad input alert
                $(btn).buttonLoader('stop');
            }
        });
    });
});