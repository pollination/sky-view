# sky-view

Recipe for computing Sky View (aka. Sky Component).

Sky View is defined as the percent of the sky dome seen by a surface. This can
be computed either using a uniform (default) sky or a cloudy sky. Note that sky
view's definition means that sky regions directly in front of the surface carry
more weight than those at the sides.

Note that computing cloudy Sky View for a vertically-oriented geometry (horizontal
sensor direction) will yield Vertical Sky Component (VSC) as described by the UK
Building Research Establishment (BRE). VSC is defined as the ratio of cloudy sky
illuminance falling on a vertical wall to the simultaneous horizontal illuminance
under an unobstructed sky [Littlefair, 1991].

Also note that this recipe still respects the transparency of objects, reducing
the percentage of the sky visible through a certain geometry by the transmittance
of that geometry.
