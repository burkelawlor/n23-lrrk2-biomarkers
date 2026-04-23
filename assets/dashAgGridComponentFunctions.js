var dagcomponentfuncs = (window.dashAgGridComponentFunctions =
  window.dashAgGridComponentFunctions || {});

dagcomponentfuncs.TestNameLink = function (props) {
  return React.createElement(
    "a",
    {
      href: props.data._analysis_url,
      target: "_blank",
      rel: "noopener noreferrer",
      style: { color: "#0d6efd", textDecoration: "none" },
    },
    props.value
  );
};
