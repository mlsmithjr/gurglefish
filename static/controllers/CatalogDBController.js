
var gurglefishApp = angular.module('gurglefish', []);

gurglefishApp.config(['$interpolateProvider', function($interpolateProvider) {
  $interpolateProvider.startSymbol('{[');
  $interpolateProvider.endSymbol(']}');
}]);

var spin_opts = {
  lines: 10 // The number of lines to draw
, length: 20 // The length of each line
, width: 11 // The line thickness
, radius: 25 // The radius of the inner circle
, scale: 1 // Scales overall size of the spinner
, corners: 1 // Corner roundness (0..1)
, color: '#000' // #rgb or #rrggbb or array of colors
, opacity: 0.25 // Opacity of the lines
, rotate: 0 // The rotation offset
, direction: 1 // 1: clockwise, -1: counterclockwise
, speed: 1 // Rounds per second
, trail: 60 // Afterglow percentage
, fps: 20 // Frames per second when using setTimeout() as a fallback for CSS
, zIndex: 2e9 // The z-index (defaults to 2000000000)
, className: 'spinner' // The CSS class to assign to the spinner
, top: '50%' // Top position relative to parent
, left: '50%' // Left position relative to parent
, shadow: false // Whether to render a shadow
, hwaccel: false // Whether to use hardware acceleration
, position: 'absolute' // Element positioning
};

gurglefishApp.controller('CatalogDBController', ['$scope','$http', function($scope,$http) {
    $scope.envlist = [];
    $scope.selectedEnv = null;
    $scope.selectedSObject = null;
    $scope.selectedSObjectFields = null;
    $scope.mappedTables = null;
    $scope.sobject = null;
    $scope.dirty = false;


    $http.get('/services/envlist').success(function(data) {
        console.log(data);
        if (!data.success) {
            $scope.message = data.message;
            $scope.errorName = data.type;
        }
        else {
            $scope.envlist = data.payload;
            $scope.message = null;
       }
    });

    $scope.handleEnvSelect = function() {

        var spinner = new Spinner(spin_opts).spin(document.getElementById('main'));

        $http.post('/services/catalog', { 'db':$scope.selectedEnv }).success(function(data) {
            spinner.stop();
            console.log(data);
            if (!data.success) {
                $scope.message = data.message;
                $scope.errorName = data.type;
                alert(data.message)
            }
            else {
                $scope.mappedTables = data.payload;
                $scope.message = null;
           }
        });
    };

    $scope.handleSObjectSelect = function(sobject) {
        if ($scope.selectedSObject == sobject) {
            return;
        }
        if (typeof sobject.fields !== 'undefined') {
            $scope.selectedSObject = sobject;
            $scope.selectedSObjectFields = sobject.fields;
            return;
        }
        var spinner = new Spinner(spin_opts).spin(document.getElementById('sobjectFieldPanel'));

        $scope.selectedSObject = sobject;
        $http.post('/services/sobject', { 'db':$scope.selectedEnv, 'sobject':$scope.selectedSObject }).success(function(data) {
            spinner.stop();
            console.log(data);
            if (!data.success) {
                $scope.message = data.message;
                $scope.errorName = data.type;
            }
            else {
                $scope.selectedSObject.fields = data.payload;
                $scope.selectedSObject.dirty = false;
                $scope.selectedSObjectFields = sobject.fields;
                $scope.message = null;
           }
        });
    };

    $scope.isTextType = function(typeName) {
        return typeName == 'string' || typeName == 'textarea';
    };

    $scope.toggleField = function(field) {
        field.selected = !field.selected;
        $scope.dirty = true;
        $scope.selectedSObject.dirty = true;
        if (field.selected == true && $scope.selectedSObject.selected == false) {
            $scope.selectedSObject.selected = true;
        }
        else {
            var flag = false;
            angular.forEach($scope.selectedSObject.fields, function(value, key) {
                if (value.selected) {
                    // at least one selected
                    flag = true;
                    return;
                }
            });
            $scope.selectedSObject.selected = flag;
        }
    };

    $scope.toggleObject = function(sobject) {
        if (sobject.selected == false) return;
        sobject.selected = !sobject.selected;
        $scope.dirty = true;
        $scope.selectedSObject.dirty = true;
        $scope.handleSObjectSelect(sobject);
        angular.forEach($scope.selectedSObject.fields, function(value, key) {
            value.selected = sobject.selected;
        });
    };

    $scope.newButton = function() {

    }

    $scope.saveButton = function() {
        var payload = new Array();
        angular.forEach($scope.mappedTables, function(value, key) {
            if (value.dirty) {
                payload.push(value);
            }
        });
        $http.post('/services/save', { 'db': $scope.selectedEnv, 'changes': payload }).success(function(data) {
            if (!data.success) {
                alert(data.message);
            }
            else {
                angular.forEach(payload, function(value, key) {
                    payload.dirty = false;
                });
                $scope.dirty = false;
                alert('changes saved');
            }
        });
    };


}]);

