from sql import plot
from sql.ggplot.geom.geom import geom
from sql.telemetry import telemetry


class geom_boxplot(geom):
    """
    Boxplot

    Parameters
    ----------
    orient : str {"h", "v"}, default="v"
        Boxplot orientation (vertical/horizontal)
    """

    def __init__(self, orient="v"):
        self.orient = orient

    @telemetry.log_call("ggplot-boxplot")
    def draw(self, gg, ax=None):
        plot.boxplot(
            table=gg.table,
            column=gg.mapping.x,
            conn=gg.conn,
            with_=gg.with_,
            ax=ax or gg.axs[0],
            orient=self.orient,
        )

        return gg
