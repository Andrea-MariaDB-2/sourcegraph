import { ApolloError } from '@apollo/client'
import React, { useEffect } from 'react'

import { LoadingSpinner } from '@sourcegraph/react-loading-spinner'
import { useSteps } from '@sourcegraph/wildcard/src/components/Steps/context'

import { UserCodeHosts } from '../../user/settings/codeHosts/UserCodeHosts'

interface CodeHostsConnection extends Omit<UserCodeHosts, 'onDidRemove' | 'onDidError'> {
    refetch: UserCodeHosts['onDidRemove']
    loading: boolean
    error: ApolloError | undefined
}

export const CodeHostsConnection: React.FunctionComponent<CodeHostsConnection> = ({
    user,
    context,
    refetch,
    externalServices,
    loading,
    error,
}) => {
    const { setComplete, currentIndex, currentStep } = useSteps()

    useEffect(() => {
        if (Array.isArray(externalServices) && externalServices.length > 0) {
            if (!currentStep.isComplete) {
                setComplete(currentIndex, true)
            }
        } else if (currentStep.isComplete) {
            setComplete(currentIndex, false)
        }
    }, [currentIndex, setComplete, externalServices, currentStep])

    if (loading) {
        return (
            <div className="d-flex justify-content-center">
                <LoadingSpinner className="icon-inline" />
            </div>
        )
    }

    if (error) {
        console.log(error)
    }

    return (
        <>
            <div className="mb-4">
                <h3>Connect with code hosts</h3>
                <p className="text-muted">
                    Connect with providers where your source code is hosted. Then, choose the repositories you’d like to
                    search with Sourcegraph.
                </p>
            </div>
            <UserCodeHosts
                user={user}
                externalServices={externalServices}
                context={context}
                onDidError={error => console.warn('<UserCodeHosts .../>', error)}
                onDidRemove={() => refetch()}
            />
        </>
    )
}
