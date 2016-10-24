package com.zanox.demo.model;

public class Membership {
    public MembershipId id;
    public String status;

    public Membership(MembershipId membershipId)
    {
        this.id = membershipId;
    }

    public Membership(MembershipId membershipId, String status)
    {
        this.id = membershipId;
        this.status = status;
    }

    public Membership activate() {
        this.status = "active";

        return this;
    }

    public Membership deactivate() {
        this.status = "inactive";

        return this;
    }
}
